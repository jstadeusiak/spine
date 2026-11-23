package spine.sequencer;

import io.aeron.archive.client.AeronArchive;
import spine.client.CatalogSubscription;
import spine.client.EventHandler;
import spine.feeds.CapturedIngress;
import spine.feeds.WebSocketCollector;
import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import spine.protocol.sbe.SequencedEventDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;

/** Test-only anonymous Spot JSON capture. Package this class and ClusterFixture in the retained executable jar. */
public final class BinanceJsonSmoke {
    private static final long LOG = 42;
    private static final String IPC = "aeron:ipc?term-length=1m";
    private static final URI ENDPOINT = URI.create("wss://data-stream.binance.vision/ws?timeUnit=MICROSECOND");
    private static final String SUBSCRIPTION = "{\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@trade\"],\"id\":1}";

    public static void main(final String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) { throw new IllegalArgumentException("DATA_DIR [SECONDS=60]"); }
        final Path data = Path.of(args[0]).toAbsolutePath();
        final long seconds = args.length == 2 ? Long.parseLong(args[1]) : 60;
        if (seconds < 1 || seconds > 86_400) { throw new IllegalArgumentException("seconds must be 1..86400"); }
        final Path bundle = Path.of(BinanceJsonSmoke.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isRegularFile(bundle)) { throw new IllegalStateException("launch the bundled helper jar"); }
        final String sha = sha256(bundle);
        final Path artifact = data.resolve("artifacts").resolve(sha + ".jar");
        Files.createDirectories(artifact.getParent());
        if (!Files.exists(artifact)) { Files.copy(bundle, artifact); }
        if (!sha.equals(sha256(artifact))) { throw new IllegalStateException("retained executable digest differs"); }
        try (ClusterFixture fixture = new ClusterFixture(data, new SequencerService(), false);
             AeronArchive archive = fixture.connectArchive(8100)) {
            fixture.expectClosedConsumerControlResponse(archive);
            initialize(fixture);
            try (CapturedIngress ingress = new CapturedIngress(archive, fixture.producerContext(), LOG, 10, 7,
                8210, 8410, null)) {
                fixture.await(ingress::idle, ingress::poll, "recovered exact proposal admission");
                final long previousHead = ingress.acknowledgedSequence(), origin = System.nanoTime();
                final int configuration = next(ingress.lastConfiguration()), clock = next(ingress.lastClockId());
                define(fixture, ingress, configuration, clock, origin, artifact, sha);
                final WebSocketCollector websocket = new WebSocketCollector(ingress, configuration, clock, origin,
                    ENDPOINT, SUBSCRIPTION, null);
                try {
                    final BackoffIdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 100_000);
                    final long end = System.nanoTime() + seconds * 1_000_000_000L;
                    while (System.nanoTime() - end < 0) {
                        fixture.assertHealthy();
                        fixture.pollAdmin();
                        idle.idle(websocket.doWork());
                    }
                    websocket.doWork();
                } finally { websocket.onClose(); }
                fixture.await(ingress::idle, ingress::poll, "final capture and admission fence");
                export(fixture, archive, data, configuration, ingress.acknowledgedSequence(), previousHead, seconds, sha);
            }
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    private static void initialize(final ClusterFixture fixture) {
        fixture.connect();
        final Wire wire = new Wire();
        final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        final byte[] reply = fixture.exchange(command, wire.encodeInitialize(command, 0, LOG));
        if (!wire.validResult(new UnsafeBuffer(reply), 0, reply.length) || wire.result.status() != AdmissionStatus.INITIALIZED) {
            throw new IllegalStateException("log initialization rejected");
        }
    }

    private static void define(final ClusterFixture fixture, final CapturedIngress ingress, final int configuration,
        final int clock, final long origin, final Path artifact, final String sha) {
        final String profile = "{\"profile\":\"spine.capture.v1\",\"venue\":\"binance-spot\",\"channel\":\"trade\","
            + "\"parser\":{\"artifact\":" + quote(artifact.toUri().toString()) + ",\"sha256\":" + quote(sha)
            + ",\"entrypoint\":" + quote(WebSocketCollector.class.getName())
            + ",\"parameters\":{\"format\":\"JSON\",\"timeUnit\":\"MICROSECOND\",\"subscription\":" + SUBSCRIPTION
            + ",\"applicationHeartbeat\":null,\"rfcPingPong\":\"JDK WebSocket implementation\",\"java\":"
            + quote(System.getProperty("java.vendor") + " " + System.getProperty("java.version")) + ",\"harness\":"
            + quote(BinanceJsonSmoke.class.getName()) + ",\"exchangeTimestamps\":\"Unix epoch microseconds\"}},"
            + "\"raw\":{\"boundary\":\"one complete WebSocket application text message\",\"representation\":\"UTF-8\","
            + "\"transformation\":\"JDK UTF-16 callbacks reassembled and strictly reencoded UTF-8; RFC controls excluded\"},"
            + "\"native\":{\"namespace\":\"binance Spot BTCUSDT trade id t\",\"representation\":\"absent; identity retained in raw JSON\"},"
            + "\"clock\":{\"domain\":\"process monotonic elapsed nanoseconds\",\"origin\":" + quote(Long.toString(origin))
            + ",\"resolutionNanos\":1,\"receive\":\"first text callback\",\"complete\":\"last text callback assembled\","
            + "\"ready\":\"immediately before capture submission\"}}";
        final String[] labels = {"binance-public", "BTCUSDT", "ws-json-" + Integer.toUnsignedString(configuration),
            WebSocketCollector.class.getName(), profile, "elapsed nanoseconds from retained origin; synchronization uncertainty unknown"};
        final byte[][] bytes = new byte[labels.length][];
        for (int i = 0; i < labels.length; i++) { bytes[i] = labels[i].getBytes(StandardCharsets.UTF_8); }
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final int length = new CaptureCodec().encodeDefinition(payload, 0, -1, bytes);
        if (!ingress.submit(configuration, clock, -1, -1, System.nanoTime() - origin, payload, 0, length)) {
            throw new IllegalStateException("new source/session definition refused");
        }
        fixture.await(ingress::idle, ingress::poll, "immutable source definition admission");
    }

    private static void export(final ClusterFixture fixture, final AeronArchive archive, final Path data,
        final int configuration, final long head, final long previousHead, final long seconds, final String sha) throws IOException {
        final String suffix = Integer.toUnsignedString(configuration);
        try (BufferedWriter frames = Files.newBufferedWriter(data.resolve("canonical-" + suffix + ".jsonl"));
             BufferedWriter definitions = Files.newBufferedWriter(data.resolve("definitions-" + suffix + ".jsonl"));
             CatalogSubscription replay = new CatalogSubscription(archive, LOG, SequencerService.OUTPUT_STREAM, 0, IPC, 8300)) {
            final Audit audit = new Audit(frames, definitions);
            final EventHandler handler = audit::onEvent;
            fixture.await(() -> replay.sequence() == head, () -> {
                if (replay.requiresRefresh()) { replay.refresh(); }
                replay.poll(handler, 256);
            }, "canonical output export");
            if (audit.sequence != head || audit.sessions.isEmpty() || audit.currentConfiguration != Integer.toUnsignedLong(configuration)
                || audit.sessions.get(audit.currentConfiguration) == 0) {
                throw new IllegalStateException("exported canonical cursor/session differs");
            }
            final String report = "{\"mode\":\"binance-spot-json\",\"endpoint\":" + quote(ENDPOINT.toString())
                + ",\"seconds\":" + seconds + ",\"canonicalHead\":" + head
                + ",\"previousCanonicalHead\":" + previousHead + ",\"configuration\":" + Integer.toUnsignedLong(configuration)
                + ",\"clockId\":" + audit.currentClock + ",\"cumulativeFrames\":" + audit.frames + ",\"cumulativeRawBytes\":" + audit.bytes
                + ",\"definitions\":" + audit.sessions.size() + ",\"currentSessionFrames\":" + audit.sessions.get(audit.currentConfiguration)
                + ",\"timestampViolations\":0,\"bundleSha256\":" + quote(sha) + "}";
            Files.writeString(data.resolve("run-" + suffix + ".json"), report + "\n");
            System.out.println(report);
        }
    }

    private static final class Audit {
        final BufferedWriter output, definitions;
        final CaptureCodec codec = new CaptureCodec();
        final Map<Long, Long> sessions = new LinkedHashMap<>();
        long sequence, frames, bytes, currentConfiguration, currentClock;
        String session;
        Audit(final BufferedWriter output, final BufferedWriter definitions) { this.output = output; this.definitions = definitions; }

        void onEvent(final DirectBuffer buffer, final int offset, final int length, final SequencedEventDecoder event) {
            if (event.logId() != LOG || event.sequence() != sequence + 1 || event.sourceId() != 7) {
                throw new IllegalStateException("noncontiguous or foreign canonical output");
            }
            final String metadata = "\"sequence\":" + event.sequence() + ",\"logId\":" + event.logId()
                + ",\"sourceId\":" + event.sourceId() + ",\"configuration\":" + event.sourceConfigVersion() + ",\"clockId\":" + event.clockId()
                + ",\"sequencedTimeNanos\":" + event.sequencedTimeNanos() + ",\"receiveTimeNanos\":" + event.receiveTimeNanos()
                + ",\"completeTimeNanos\":" + event.completeTimeNanos() + ",\"readyTimeNanos\":" + event.readyTimeNanos();
            final int body = offset + Wire.EVENT_OVERHEAD, size = length - Wire.EVENT_OVERHEAD;
            final String row;
            if (codec.validDefinition(buffer, body, size)) {
                if (event.sourceConfigVersion() <= currentConfiguration || event.clockId() <= currentClock
                    || event.receiveTimeNanos() != -1 || event.completeTimeNanos() != -1 || event.readyTimeNanos() < 0) {
                    throw new IllegalStateException("source bindings did not advance or definition times differ");
                }
                currentConfiguration = event.sourceConfigVersion(); currentClock = event.clockId();
                sessions.put(currentConfiguration, 0L);
                final String[] labels = new String[6];
                for (int i = 0; i < labels.length; i++) { labels[i] = utf8(buffer, codec.labelOffset(i), codec.labelLength(i)); }
                session = labels[2];
                row = "{\"kind\":\"definition\"," + metadata + ",\"provider\":" + quote(labels[0]) + ",\"product\":" + quote(labels[1])
                    + ",\"session\":" + quote(session) + ",\"collector\":" + quote(labels[3]) + ",\"profile\":" + quote(labels[4])
                    + ",\"clockMeaning\":" + quote(labels[5]) + ",\"syncUncertaintyNanos\":" + codec.definition.syncUncertaintyNanos() + "}";
                write(definitions, row);
            } else if (codec.validObservation(buffer, body, size)) {
                if (event.sourceConfigVersion() != currentConfiguration || event.clockId() != currentClock
                    || event.receiveTimeNanos() < 0 || event.receiveTimeNanos() > event.completeTimeNanos()
                    || event.completeTimeNanos() > event.readyTimeNanos()
                    || codec.observation.rawFrameSequence() != sessions.getOrDefault(currentConfiguration, 0L) + 1
                    || codec.observation.subeventIndex() != 0) { throw new IllegalStateException("invalid raw frame binding/sequence/times"); }
                sessions.put(currentConfiguration, codec.observation.rawFrameSequence());
                frames++; bytes += codec.rawLength;
                row = "{\"kind\":\"observation\"," + metadata + ",\"session\":" + quote(session)
                    + ",\"rawFrameSequence\":" + codec.observation.rawFrameSequence() + ",\"rawByteLength\":" + codec.rawLength
                    + ",\"nativeNumericId\":" + quote(Long.toUnsignedString(codec.observation.nativeNumericId()))
                    + ",\"raw\":" + quote(utf8(buffer, codec.rawOffset, codec.rawLength)) + "}";
            } else { throw new IllegalStateException("unknown capture payload"); }
            write(output, row);
            sequence = event.sequence();
        }
    }

    private static int next(final long value) {
        if (value >= 0xFFFF_FFFEL) { throw new IllegalStateException("source binding exhausted"); }
        return (int)(value + 1);
    }
    private static String sha256(final Path file) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }
    private static String utf8(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] bytes = new byte[length]; buffer.getBytes(offset, bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (final java.nio.charset.CharacterCodingException invalid) { throw new IllegalStateException("invalid captured UTF-8", invalid); }
    }
    private static void write(final BufferedWriter writer, final String row) {
        try { writer.write(row); writer.newLine(); } catch (final IOException failure) { throw new UncheckedIOException(failure); }
    }
    private static String quote(final String value) {
        final StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if (c == '"' || c == '\\') { result.append('\\').append(c); }
            else if (c < 32) { result.append(String.format("\\u%04x", (int)c)); }
            else { result.append(c); }
        }
        return result.append('"').toString();
    }
}
