package io.github.jstadeusiak.spine.feeds;

import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/** One text-message mailbox; JDK network allocation is outside the owned capture/assembly path. */
public final class WebSocketCollector implements Agent {
    static final int MAX_RAW_BYTES = Wire.MAX_PAYLOAD_BYTES - CaptureCodec.OBSERVATION_OVERHEAD;
    private static final long PING_INTERVAL_NS = 20_000_000_000L, SEND_TIMEOUT_NS = 10_000_000_000L;
    private final CapturedIngress ingress;
    private final int configuration, clock;
    private final String heartbeat;
    private final Receiver receiver;
    private final HttpClient http;
    private final CaptureCodec codec = new CaptureCodec();
    private final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
    private CompletableFuture<WebSocket> ping;
    private long frameSequence = 1, lastPing = System.nanoTime();

    /** The caller admits this immutable source/session definition before construction and owns ingress. */
    public WebSocketCollector(final CapturedIngress ingress, final int configuration, final int clock,
        final long clockOrigin, final URI endpoint, final String subscription) throws Exception {
        this(ingress, configuration, clock, clockOrigin, endpoint, subscription, "{\"method\":\"ping\"}");
    }

    public WebSocketCollector(final CapturedIngress ingress, final int configuration, final int clock,
        final long clockOrigin, final URI endpoint, final String subscription, final String heartbeat) throws Exception {
        this(ingress, configuration, clock, clockOrigin, endpoint, subscription, heartbeat, Map.of(), false);
    }

    /** Tagged mode prefixes every application message with 0 for UTF-8 text or 1 for unchanged binary bytes. */
    public WebSocketCollector(final CapturedIngress ingress, final int configuration, final int clock,
        final long clockOrigin, final URI endpoint, final String subscription, final String heartbeat,
        final Map<String, String> headers, final boolean taggedBinary) throws Exception {
        this.ingress = Objects.requireNonNull(ingress);
        if (configuration == 0 || configuration == -1 || clock == 0 || clock == -1 || !ingress.idle()
            || ingress.lastConfiguration() != Integer.toUnsignedLong(configuration)
            || ingress.lastClockId() != Integer.toUnsignedLong(clock)) {
            throw new IllegalArgumentException("acknowledged source binding required before WebSocket open");
        }
        Objects.requireNonNull(endpoint);
        Objects.requireNonNull(subscription);
        this.configuration = configuration;
        this.clock = clock;
        this.heartbeat = heartbeat;
        receiver = new Receiver(clockOrigin, taggedBinary);
        http = HttpClient.newHttpClient();
        try {
            final WebSocket.Builder builder = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(10));
            Objects.requireNonNull(headers).forEach(builder::header);
            builder.buildAsync(endpoint, receiver)
                .get(10, TimeUnit.SECONDS).sendText(subscription, true)
                .get(10, TimeUnit.SECONDS);
        } catch (final Exception failure) {
            onClose();
            if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            throw failure;
        }
    }

    @Override public int doWork() {
        if (receiver.failure != null) { throw new AgentTerminationException(receiver.failure); }
        int work = ingress.poll();
        if (receiver.ready && ingress.idle()) {
            if (frameSequence <= 0) { throw new AgentTerminationException("raw frame sequence exhausted"); }
            final long ready = System.nanoTime() - receiver.origin;
            if (ready < receiver.complete) { throw new AgentTerminationException("collector clock moved backwards"); }
            final int length = codec.encodeObservation(payload, 0, frameSequence, 0, CaptureCodec.UNKNOWN_NATIVE_ID,
                null, 0, 0, receiver.raw, 0, receiver.length);
            if (ingress.submit(configuration, clock, receiver.receive, receiver.complete, ready, payload, 0, length)) {
                frameSequence++;
                receiver.release();
                work++;
            }
        }
        final long now = System.nanoTime();
        if (ping != null) {
            if (ping.isDone()) {
                try { ping.join(); } catch (final RuntimeException failure) { throw new AgentTerminationException(failure); }
                ping = null;
                work++;
            } else if (now - lastPing >= SEND_TIMEOUT_NS) { throw new AgentTerminationException("WebSocket ping send timeout"); }
        }
        if (heartbeat != null && ping == null && now - lastPing >= PING_INTERVAL_NS) {
            try { ping = receiver.socket.sendText(heartbeat, true); }
            catch (final RuntimeException failure) { throw new AgentTerminationException(failure); }
            lastPing = now;
            work++;
        }
        return work;
    }

    @Override public void onClose() {
        receiver.closing = true;
        if (receiver.socket != null) { receiver.socket.abort(); }
        http.shutdownNow();
    }
    @Override public String roleName() { return "spine-websocket-collector"; }

    /** Listener writes bytes/times; the Agent borrows them only while ready and releases demand after copying. */
    static final class Receiver implements WebSocket.Listener {
        final UnsafeBuffer raw = new UnsafeBuffer(new byte[MAX_RAW_BYTES]);
        final long origin;
        final boolean taggedBinary;
        volatile WebSocket socket;
        volatile boolean ready, closing;
        volatile Throwable failure;
        int length;
        long receive, complete;
        private boolean inMessage, binary;
        private char high;

        Receiver(final long origin) { this(origin, false); }
        Receiver(final long origin, final boolean taggedBinary) { this.origin = origin; this.taggedBinary = taggedBinary; }
        @Override public void onOpen(final WebSocket socket) {
            this.socket = socket;
            if (closing) { socket.abort(); } else { socket.request(1); }
        }
        @Override public CompletionStage<?> onText(final WebSocket socket, final CharSequence text, final boolean last) {
            if (!begin(false)) { return null; }
            for (int i = 0; i < text.length() && failure == null; i++) {
                final char value = text.charAt(i);
                if (high != 0) {
                    if (!Character.isLowSurrogate(value)) { fail("invalid UTF-16 surrogate"); break; }
                    append(Character.toCodePoint(high, value));
                    high = 0;
                } else if (Character.isHighSurrogate(value)) { high = value; }
                else if (Character.isLowSurrogate(value)) { fail("invalid UTF-16 surrogate"); }
                else { append(value); }
            }
            if (failure != null) { return null; }
            if (last && high != 0) { fail("incomplete UTF-16 surrogate"); return null; }
            return finish(socket, last);
        }
        private boolean begin(final boolean kind) {
            if (closing || failure != null) { return false; }
            if (ready) { fail("WebSocket exceeded message demand"); return false; }
            if (inMessage && binary != kind) { fail("interleaved WebSocket message kinds"); return false; }
            if (!inMessage) {
                receive = System.nanoTime() - origin;
                if (receive < 0) { fail("collector clock origin is in the future"); return false; }
                length = taggedBinary ? 1 : 0;
                if (taggedBinary) { raw.putByte(0, (byte)(kind ? 1 : 0)); }
                inMessage = true;
                binary = kind;
            }
            return true;
        }
        private CompletionStage<?> finish(final WebSocket socket, final boolean last) {
            if (last) {
                complete = System.nanoTime() - origin;
                inMessage = false;
                ready = true;
            } else { socket.request(1); }
            return null;
        }
        private void append(final int value) {
            final int count = value < 0x80 ? 1 : value < 0x800 ? 2 : value < 0x10000 ? 3 : 4;
            if (length > MAX_RAW_BYTES - count) { fail("WebSocket text exceeds capture bound"); return; }
            if (count == 1) { raw.putByte(length++, (byte)value); return; }
            if (count == 4) { raw.putByte(length++, (byte)(0xF0 | value >>> 18)); }
            if (count >= 3) { raw.putByte(length++, (byte)((count == 3 ? 0xE0 : 0x80) | (value >>> 12 & 0x3F))); }
            raw.putByte(length++, (byte)((count == 2 ? 0xC0 : 0x80) | (value >>> 6 & 0x3F)));
            raw.putByte(length++, (byte)(0x80 | value & 0x3F));
        }
        void release() { ready = false; socket.request(1); }
        @Override public CompletionStage<?> onPing(final WebSocket socket, final ByteBuffer message) {
            if (!closing && failure == null && !ready) { socket.request(1); }
            return null;
        }
        @Override public CompletionStage<?> onPong(final WebSocket socket, final ByteBuffer message) {
            return onPing(socket, message);
        }
        @Override public CompletionStage<?> onBinary(final WebSocket socket, final ByteBuffer data, final boolean last) {
            if (!taggedBinary) { if (!closing) { fail("binary WebSocket messages are unsupported"); } return null; }
            if (!begin(true)) { return null; }
            final int count = data.remaining();
            if (count > MAX_RAW_BYTES - length) { fail("WebSocket binary exceeds capture bound"); return null; }
            raw.putBytes(length, data, data.position(), count);
            length += count;
            return finish(socket, last);
        }
        @Override public CompletionStage<?> onClose(final WebSocket socket, final int code, final String reason) {
            if (!closing) { fail("upstream WebSocket closed"); }
            return null;
        }
        @Override public void onError(final WebSocket socket, final Throwable error) {
            if (!closing && failure == null) { failure = error; }
        }
        private void fail(final String message) {
            if (failure == null) { failure = new IllegalStateException(message); }
        }
    }
}
