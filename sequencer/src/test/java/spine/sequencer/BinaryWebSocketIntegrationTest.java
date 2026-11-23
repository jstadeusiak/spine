package spine.sequencer;

import io.aeron.archive.client.AeronArchive;
import spine.client.CatalogSubscription;
import spine.feeds.CapturedIngress;
import spine.feeds.WebSocketCollector;
import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real JDK handshake/demand/pong plus maximum binary capture, without a live account. */
class BinaryWebSocketIntegrationTest {
    @TempDir Path directory;
    private static final String SUBSCRIBE = "{\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@trade\"],\"id\":1}";
    private static final byte[] ACK = "{\"result\":null,\"id\":1}".getBytes(StandardCharsets.UTF_8);

    @Test @Timeout(60)
    void authenticatedMixedFramesAndAutomaticPongSurviveExactCaptureAndReplay() throws Exception {
        final byte[] binary = new byte[65_499];
        for (int i = 0; i < binary.length; i++) { binary[i] = (byte)(i * 31); }
        try (Peer peer = new Peer(binary);
             ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true);
             AeronArchive archive = fixture.connectArchive(8100)) {
            fixture.expectClosedConsumerControlResponse(archive);
            fixture.connect();
            final Wire wire = new Wire();
            final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            fixture.exchange(command, wire.encodeInitialize(command, 0, 42));
            try (CapturedIngress ingress = new CapturedIngress(archive, fixture.producerContext(), 42, 10, 7,
                8210, 8410, null)) {
                final long origin = System.nanoTime();
                final byte[][] labels = {"local-peer".getBytes(StandardCharsets.UTF_8), "BTCUSDT".getBytes(StandardCharsets.UTF_8),
                    "session-1".getBytes(StandardCharsets.UTF_8), "tagged-websocket-test".getBytes(StandardCharsets.UTF_8),
                    "{}".getBytes(StandardCharsets.UTF_8), "elapsed nanoseconds".getBytes(StandardCharsets.UTF_8)};
                final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
                final CaptureCodec codec = new CaptureCodec();
                final int definition = codec.encodeDefinition(payload, 0, -1, labels);
                assertThat(ingress.submit(1, 1, -1, -1, 0, payload, 0, definition)).isTrue();
                fixture.await(ingress::idle, ingress::poll, "source definition admission");
                final WebSocketCollector collector = new WebSocketCollector(ingress, 1, 1, origin, peer.uri(),
                    SUBSCRIBE, null, Map.of("X-MBX-APIKEY", "local-test-key"), true);
                try {
                    fixture.await(() -> ingress.acknowledgedSequence() == 3, () -> {
                        peer.assertHealthy();
                        collector.doWork();
                    }, "text control and maximum fragmented binary admission");
                    assertThat(peer.pong.await(5, TimeUnit.SECONDS)).as("RFC pong copied the ping payload").isTrue();
                } finally { collector.onClose(); }
                peer.assertHealthy();
                fixture.takeSnapshot();
                final List<byte[]> observed = new ArrayList<>();
                try (CatalogSubscription replay = new CatalogSubscription(archive, 42, SequencerService.OUTPUT_STREAM,
                    0, "aeron:ipc?term-length=1m", 8300)) {
                    fixture.await(() -> replay.sequence() == 3, () -> replay.poll((buffer, offset, length, event) -> {
                        final int body = offset + Wire.EVENT_OVERHEAD, size = length - Wire.EVENT_OVERHEAD;
                        if (!codec.validObservation(buffer, body, size)) { return; }
                        assertThat(event.sourceId()).isEqualTo(7);
                        assertThat(event.sourceConfigVersion()).isEqualTo(1);
                        assertThat(event.clockId()).isEqualTo(1);
                        assertThat(event.receiveTimeNanos()).isBetween(0L, event.completeTimeNanos());
                        assertThat(event.completeTimeNanos()).isLessThanOrEqualTo(event.readyTimeNanos());
                        assertThat(codec.observation.rawFrameSequence()).isEqualTo(observed.size() + 1);
                        final byte[] raw = new byte[codec.rawLength];
                        buffer.getBytes(codec.rawOffset, raw);
                        observed.add(raw);
                    }, 256), "recorded exact mixed frame replay");
                }
                assertThat(observed).hasSize(2);
                assertThat(observed.get(0)[0]).isZero();
                assertThat(Arrays.copyOfRange(observed.get(0), 1, observed.get(0).length)).containsExactly(ACK);
                assertThat(observed.get(1)[0]).isEqualTo((byte)1);
                assertThat(observed.get(1)).hasSize(65_500);
                assertThat(Arrays.copyOfRange(observed.get(1), 1, observed.get(1).length)).containsExactly(binary);
            }
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    private static final class Peer implements AutoCloseable {
        final ServerSocket listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        final CountDownLatch stop = new CountDownLatch(1), pong = new CountDownLatch(1);
        final Thread thread;
        volatile Throwable failure;
        volatile Socket socket;

        Peer(final byte[] binary) throws Exception {
            listener.setSoTimeout(10_000);
            thread = Thread.ofPlatform().name("test-websocket-peer").start(() -> {
                try (Socket connected = listener.accept()) {
                    socket = connected;
                    connected.setSoTimeout(10_000);
                    final InputStream input = connected.getInputStream();
                    final OutputStream output = connected.getOutputStream();
                    final String request = headers(input);
                    assertThat(request).contains("GET /ws HTTP/1.1");
                    assertThat(request.toLowerCase()).contains("x-mbx-apikey: local-test-key\r\n");
                    final String key = Arrays.stream(request.split("\r\n"))
                        .filter(line -> line.toLowerCase().startsWith("sec-websocket-key:"))
                        .map(line -> line.substring(line.indexOf(':') + 1).trim()).findFirst().orElseThrow();
                    final String accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1")
                        .digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)));
                    output.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                        + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                    output.flush();
                    assertThat(readFrame(input, 1)).containsExactly(SUBSCRIBE.getBytes(StandardCharsets.UTF_8));
                    send(output, true, 1, ACK, 0, ACK.length);
                    send(output, false, 2, binary, 0, 17);
                    final byte[] ping = {(byte)0xFF, 0, 42};
                    send(output, true, 9, ping, 0, ping.length);
                    send(output, true, 0, binary, 17, binary.length - 17);
                    assertThat(readFrame(input, 10)).containsExactly(ping);
                    pong.countDown();
                    stop.await(20, TimeUnit.SECONDS);
                } catch (final Throwable problem) { failure = problem; }
            });
        }

        URI uri() { return URI.create("ws://127.0.0.1:" + listener.getLocalPort() + "/ws"); }
        void assertHealthy() {
            if (failure != null) { throw new AssertionError("local WebSocket peer failed", failure); }
        }
        @Override public void close() throws Exception {
            stop.countDown();
            listener.close();
            if (socket != null) { socket.close(); }
            thread.join(10_000);
            assertThat(thread.isAlive()).isFalse();
            assertHealthy();
        }

        private static String headers(final InputStream input) throws Exception {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            int tail = 0;
            while (bytes.size() < 16_384) {
                final int value = octet(input);
                bytes.write(value);
                tail = tail << 8 | value;
                if (tail == 0x0D0A0D0A) { return bytes.toString(StandardCharsets.US_ASCII); }
            }
            throw new IllegalStateException("oversized handshake");
        }
        private static byte[] readFrame(final InputStream input, final int opcode) throws Exception {
            final int first = octet(input), second = octet(input);
            assertThat(first).isEqualTo(0x80 | opcode);
            assertThat(second & 0x80).isEqualTo(0x80);
            long length = second & 0x7F;
            if (length == 126) { length = octet(input) << 8 | octet(input); }
            else if (length == 127) {
                length = 0;
                for (int i = 0; i < 8; i++) { length = length << 8 | octet(input); }
            }
            assertThat(length).isBetween(0L, 65_500L);
            final byte[] mask = input.readNBytes(4), payload = input.readNBytes((int)length);
            assertThat(mask).hasSize(4);
            assertThat(payload).hasSize((int)length);
            for (int i = 0; i < payload.length; i++) { payload[i] ^= mask[i & 3]; }
            return payload;
        }
        private static void send(final OutputStream output, final boolean last, final int opcode,
            final byte[] bytes, final int offset, final int length) throws Exception {
            output.write((last ? 0x80 : 0) | opcode);
            if (length < 126) { output.write(length); }
            else { output.write(126); output.write(length >>> 8); output.write(length & 0xFF); }
            output.write(bytes, offset, length);
            output.flush();
        }
        private static int octet(final InputStream input) throws Exception {
            final int value = input.read();
            if (value < 0) { throw new EOFException("WebSocket peer disconnected"); }
            return value;
        }
    }
}
