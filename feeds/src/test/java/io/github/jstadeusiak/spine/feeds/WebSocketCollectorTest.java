package io.github.jstadeusiak.spine.feeds;

import com.sun.management.ThreadMXBean;
import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class WebSocketCollectorTest {
    private static volatile Object escaped;

    @Test void assemblesExactUtf8AcrossFragmentsAndWithholdsNextMessageDemandUntilReleased() {
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime());
        final Socket socket = new Socket();
        receiver.onOpen(socket);
        assertThat(socket.requests).isEqualTo(1);
        assertThat(receiver.onText(socket, "{\"x\":\"é中\uD83D", false)).isNull();
        final long received = receiver.receive;
        assertThat(receiver.ready).isFalse();
        assertThat(socket.requests).isEqualTo(2);
        receiver.onText(socket, "\uDE00\"}", true);
        assertThat(receiver.failure).isNull();
        assertThat(receiver.ready).isTrue();
        assertThat(receiver.receive).isEqualTo(received).isNotNegative();
        assertThat(receiver.complete).isGreaterThanOrEqualTo(received);
        assertThat(bytes(receiver)).containsExactly("{\"x\":\"é中😀\"}".getBytes(StandardCharsets.UTF_8));
        assertThat(socket.requests).isEqualTo(2);
        final byte[] first = bytes(receiver);
        receiver.release();
        assertThat(socket.requests).isEqualTo(3);
        assertThat(bytes(receiver)).containsExactly(first);
        receiver.onText(socket, "{\"channel\":\"pong\"}", true);
        assertThat(bytes(receiver)).containsExactly("{\"channel\":\"pong\"}".getBytes(StandardCharsets.UTF_8));
        assertThat(receiver.receive).isGreaterThanOrEqualTo(received);
        assertThat(socket.requests).isEqualTo(3);
    }

    @Test void preservesEmptyAndMalformedApplicationTextRatherThanInterpretingIt() {
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime());
        final Socket socket = new Socket();
        receiver.onOpen(socket);
        receiver.onText(socket, "", false);
        receiver.onText(socket, "", true);
        assertThat(receiver.ready).isTrue();
        assertThat(receiver.length).isZero();
        receiver.release();
        receiver.onText(socket, "{not json", true);
        assertThat(receiver.ready).isTrue();
        assertThat(receiver.failure).isNull();
        assertThat(bytes(receiver)).containsExactly("{not json".getBytes(StandardCharsets.UTF_8));
    }

    @Test void acceptsMaximumRawBytesAndRejectsOneMoreByteWithoutTruncatingOrPublishingIt() {
        final String maximum = "x".repeat(WebSocketCollector.MAX_RAW_BYTES);
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver full = new WebSocketCollector.Receiver(System.nanoTime());
        full.onOpen(socket);
        full.onText(socket, maximum, true);
        assertThat(full.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(full.ready).isTrue();
        assertThat(full.failure).isNull();
        assertThat(bytes(full)).containsExactly(maximum.getBytes(StandardCharsets.UTF_8));

        final WebSocketCollector.Receiver overflow = new WebSocketCollector.Receiver(System.nanoTime());
        overflow.onOpen(socket);
        overflow.onText(socket, maximum, false);
        final long requests = socket.requests;
        overflow.onText(socket, "x", true);
        assertThat(overflow.failure).hasMessageContaining("exceeds capture bound");
        assertThat(overflow.ready).isFalse();
        assertThat(socket.requests).isEqualTo(requests);
        overflow.onText(socket, "later", true);
        assertThat(overflow.ready).isFalse();
        assertThat(overflow.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
    }

    @Test void boundsUnicodeByEncodedBytesAndNeverWritesHalfAnEncodedCodePoint() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver exact = new WebSocketCollector.Receiver(System.nanoTime());
        exact.onOpen(socket);
        exact.onText(socket, "é".repeat(WebSocketCollector.MAX_RAW_BYTES / 2), true);
        assertThat(exact.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(exact.failure).isNull();
        final WebSocketCollector.Receiver overflow = new WebSocketCollector.Receiver(System.nanoTime());
        overflow.onOpen(socket);
        overflow.onText(socket, "x".repeat(WebSocketCollector.MAX_RAW_BYTES - 1), false);
        overflow.onText(socket, "é", true);
        assertThat(overflow.failure).hasMessageContaining("exceeds capture bound");
        assertThat(overflow.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES - 1);
        assertThat(overflow.ready).isFalse();
    }

    @Test void rejectsUnpairedSurrogatesIncludingAcrossFragmentBoundaries() {
        for (final String[] fragments : new String[][]{{"\uDE00"}, {"\uD83D"}, {"\uD83D", "x"}, {"\uD83D", "\uD83D"}}) {
            final Socket socket = new Socket();
            final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime());
            receiver.onOpen(socket);
            for (int i = 0; i < fragments.length; i++) { receiver.onText(socket, fragments[i], i == fragments.length - 1); }
            assertThat(receiver.failure).hasMessageContaining("UTF-16 surrogate");
            assertThat(receiver.ready).isFalse();
            assertThat(receiver.length).isZero();
        }
    }

    @Test void controlCallbacksPreservePartialTextAndDoNotReleaseACompleteMailbox() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime());
        receiver.onOpen(socket);
        receiver.onText(socket, "a", false);
        final long received = receiver.receive;
        receiver.onPing(socket, ByteBuffer.wrap(new byte[]{1}));
        receiver.onPong(socket, ByteBuffer.wrap(new byte[]{2}));
        assertThat(socket.requests).isEqualTo(4);
        receiver.onText(socket, "b", true);
        assertThat(receiver.receive).isEqualTo(received);
        assertThat(bytes(receiver)).containsExactly((byte)'a', (byte)'b');
        receiver.onPing(socket, ByteBuffer.allocate(0));
        receiver.onPong(socket, ByteBuffer.allocate(0));
        assertThat(socket.requests).isEqualTo(4);
        assertThat(receiver.ready).isTrue();
    }

    @Test void extraMessageCannotOverwriteBorrowedBytesAndTerminalFailuresRemainLatched() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime());
        receiver.onOpen(socket);
        receiver.onText(socket, "first", true);
        receiver.onText(socket, "second", true);
        final Throwable failure = receiver.failure;
        assertThat(failure).hasMessageContaining("exceeded message demand");
        assertThat(bytes(receiver)).containsExactly("first".getBytes(StandardCharsets.UTF_8));
        receiver.onError(socket, new IllegalStateException("later"));
        assertThat(receiver.failure).isSameAs(failure);

        final WebSocketCollector.Receiver binary = new WebSocketCollector.Receiver(System.nanoTime());
        binary.onBinary(socket, ByteBuffer.allocate(0), true);
        assertThat(binary.failure).hasMessageContaining("binary");
        final WebSocketCollector.Receiver closed = new WebSocketCollector.Receiver(System.nanoTime());
        closed.onClose(socket, 1000, "upstream normal close");
        assertThat(closed.failure).hasMessageContaining("upstream WebSocket closed");
        final WebSocketCollector.Receiver errored = new WebSocketCollector.Receiver(System.nanoTime());
        final Throwable original = new IllegalArgumentException("connection lost");
        errored.onError(socket, original);
        assertThat(errored.failure).isSameAs(original);
    }

    @Test void deliberateShutdownAbortsLateOpenAndDoesNotBecomeAnUpstreamFailure() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime());
        receiver.closing = true;
        receiver.onOpen(socket);
        receiver.onError(socket, new IllegalStateException("abort"));
        receiver.onClose(socket, 1000, "abort");
        receiver.onText(socket, "ignored", true);
        receiver.onBinary(socket, ByteBuffer.allocate(0), true);
        assertThat(socket.aborted).isTrue();
        assertThat(socket.requests).isZero();
        assertThat(receiver.failure).isNull();
        assertThat(receiver.ready).isFalse();
    }

    @Test void futureClockOriginFailsInsteadOfPublishingNegativeReceiveTime() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime() + 60_000_000_000L);
        receiver.onOpen(socket);
        receiver.onText(socket, "x", true);
        assertThat(receiver.failure).hasMessageContaining("clock origin is in the future");
        assertThat(receiver.ready).isFalse();
    }

    @Test void taggedBinaryPreservesRemainingDirectAndReadOnlySlicedBytesPositionsAndFragmentTimes() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime(), true);
        receiver.onOpen(socket);
        final ByteBuffer storage = ByteBuffer.allocateDirect(6);
        storage.put(new byte[]{90, 91, 0, (byte)0x80, (byte)0xFF, 92}).flip();
        final ByteBuffer first = storage.asReadOnlyBuffer();
        first.position(2).limit(5);
        final ByteBuffer backing = ByteBuffer.wrap(new byte[]{99, 98, 97, 1, 2, 3, 96});
        backing.position(2).limit(6);
        final ByteBuffer last = backing.slice().asReadOnlyBuffer();
        last.position(1).limit(4);
        assertThat(last.hasArray()).isFalse();
        assertThat(receiver.onBinary(socket, first, false)).isNull();
        final long received = receiver.receive;
        assertThat(receiver.ready).isFalse();
        receiver.onPing(socket, ByteBuffer.wrap(new byte[]{4}));
        receiver.onPong(socket, ByteBuffer.wrap(new byte[]{5}));
        receiver.onBinary(socket, last, true);
        assertThat(receiver.ready).isTrue();
        assertThat(receiver.failure).isNull();
        assertThat(receiver.receive).isEqualTo(received).isNotNegative();
        assertThat(receiver.complete).isGreaterThanOrEqualTo(received);
        assertThat(bytes(receiver)).containsExactly((byte)1, (byte)0, (byte)0x80, (byte)0xFF, (byte)1, (byte)2, (byte)3);
        assertThat(first.position()).isEqualTo(2);
        assertThat(first.limit()).isEqualTo(5);
        assertThat(last.position()).isEqualTo(1);
        assertThat(last.limit()).isEqualTo(4);
        assertThat(backing.position()).isEqualTo(2);
        assertThat(socket.requests).isEqualTo(4);
        receiver.onPing(socket, ByteBuffer.allocate(0));
        assertThat(socket.requests).isEqualTo(4);
        receiver.release();
        assertThat(socket.requests).isEqualTo(5);
    }

    @Test void taggedModeDistinguishesTextControlsMalformedTextEmptyBinaryAndSuccessiveMessages() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime(), true);
        receiver.onOpen(socket);
        receiver.onText(socket, "{\"result\":null,\"id\":1}", true);
        assertThat(bytes(receiver)).containsExactly(tagged((byte)0, "{\"result\":null,\"id\":1}".getBytes(StandardCharsets.UTF_8)));
        receiver.release();
        receiver.onBinary(socket, ByteBuffer.allocate(0), false);
        receiver.onBinary(socket, ByteBuffer.wrap(new byte[]{0, (byte)0xFF}), true);
        assertThat(bytes(receiver)).containsExactly((byte)1, (byte)0, (byte)0xFF);
        receiver.release();
        receiver.onText(socket, "é中\uD83D", false);
        receiver.onText(socket, "\uDE00{bad json", true);
        assertThat(bytes(receiver)).containsExactly(tagged((byte)0, "é中😀{bad json".getBytes(StandardCharsets.UTF_8)));
        receiver.release();
        receiver.onBinary(socket, ByteBuffer.allocate(0), true);
        assertThat(bytes(receiver)).containsExactly((byte)1);
        receiver.release();
        receiver.onText(socket, "", true);
        assertThat(bytes(receiver)).containsExactly((byte)0);
        assertThat(receiver.failure).isNull();
        assertThat(receiver.ready).isTrue();
    }

    @Test void tagConsumesOneByteOfTheBoundForTextAndBinaryAndOverflowRemainsTerminal() {
        final Socket socket = new Socket();
        final ByteBuffer maximum = ByteBuffer.allocateDirect(WebSocketCollector.MAX_RAW_BYTES - 1);
        for (int i = 0; i < maximum.capacity(); i++) { maximum.put(i, (byte)i); }
        final WebSocketCollector.Receiver exact = new WebSocketCollector.Receiver(System.nanoTime(), true);
        exact.onOpen(socket);
        exact.onBinary(socket, maximum, true);
        assertThat(exact.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(exact.raw.getByte(0)).isEqualTo((byte)1);
        for (int i = 0; i < maximum.capacity(); i++) { assertThat(exact.raw.getByte(i + 1)).isEqualTo((byte)i); }
        assertThat(exact.ready).isTrue();
        assertThat(exact.failure).isNull();
        exact.release();
        exact.onText(socket, "x".repeat(WebSocketCollector.MAX_RAW_BYTES - 1), true);
        assertThat(exact.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(exact.raw.getByte(0)).isZero();
        assertThat(exact.failure).isNull();

        final WebSocketCollector.Receiver overflow = new WebSocketCollector.Receiver(System.nanoTime(), true);
        overflow.onOpen(socket);
        overflow.onBinary(socket, maximum, false);
        final byte[] partial = bytes(overflow);
        final long requests = socket.requests;
        overflow.onBinary(socket, ByteBuffer.wrap(new byte[]{1}), true);
        final Throwable failure = overflow.failure;
        assertThat(failure).hasMessageContaining("exceeds capture bound");
        assertThat(overflow.ready).isFalse();
        assertThat(bytes(overflow)).containsExactly(partial);
        overflow.onBinary(socket, maximum, true);
        overflow.onText(socket, "later", true);
        overflow.onPing(socket, maximum);
        assertThat(overflow.failure).isSameAs(failure);
        assertThat(bytes(overflow)).containsExactly(partial);
        assertThat(socket.requests).isEqualTo(requests);

        final WebSocketCollector.Receiver tooLarge = new WebSocketCollector.Receiver(System.nanoTime(), true);
        tooLarge.onOpen(socket);
        tooLarge.onBinary(socket, ByteBuffer.allocate(WebSocketCollector.MAX_RAW_BYTES), true);
        assertThat(tooLarge.failure).hasMessageContaining("exceeds capture bound");
        assertThat(tooLarge.length).isEqualTo(1);
        assertThat(tooLarge.ready).isFalse();
        final WebSocketCollector.Receiver textOverflow = new WebSocketCollector.Receiver(System.nanoTime(), true);
        textOverflow.onOpen(socket);
        textOverflow.onText(socket, "x".repeat(WebSocketCollector.MAX_RAW_BYTES), true);
        assertThat(textOverflow.failure).hasMessageContaining("exceeds capture bound");
        assertThat(textOverflow.ready).isFalse();
    }

    @Test void interleavedFragmentKindsFailWithoutChangingPartialBytesTimesOrDemand() {
        for (final boolean binaryFirst : new boolean[]{false, true}) {
            final Socket socket = new Socket();
            final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime(), true);
            receiver.onOpen(socket);
            if (binaryFirst) { receiver.onBinary(socket, ByteBuffer.wrap(new byte[]{10, 20}), false); }
            else { receiver.onText(socket, "text\uD83D", false); }
            final byte[] partial = bytes(receiver);
            final long received = receiver.receive, requests = socket.requests;
            if (binaryFirst) { receiver.onText(socket, "text", true); }
            else { receiver.onBinary(socket, ByteBuffer.wrap(new byte[]{30}), true); }
            final Throwable failure = receiver.failure;
            assertThat(failure).hasMessageContaining("interleaved WebSocket message kinds");
            assertThat(receiver.ready).isFalse();
            assertThat(bytes(receiver)).containsExactly(partial);
            assertThat(receiver.receive).isEqualTo(received);
            receiver.onText(socket, "\uDE00", true);
            receiver.onBinary(socket, ByteBuffer.wrap(new byte[]{40}), true);
            receiver.onError(socket, new IllegalStateException("later"));
            assertThat(receiver.failure).isSameAs(failure);
            assertThat(bytes(receiver)).containsExactly(partial);
            assertThat(socket.requests).isEqualTo(requests);
        }
    }

    @Test void taggedDemandAndShutdownProtectACompletedBinaryMailbox() {
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime(), true);
        receiver.onOpen(socket);
        receiver.onBinary(socket, ByteBuffer.wrap(new byte[]{11, 22}), true);
        final byte[] accepted = bytes(receiver);
        final long receive = receiver.receive, complete = receiver.complete;
        receiver.onText(socket, "unexpected", true);
        assertThat(receiver.failure).hasMessageContaining("exceeded message demand");
        receiver.onBinary(socket, ByteBuffer.wrap(new byte[]{33}), true);
        assertThat(bytes(receiver)).containsExactly(accepted);
        assertThat(receiver.receive).isEqualTo(receive);
        assertThat(receiver.complete).isEqualTo(complete);

        final WebSocketCollector.Receiver stopped = new WebSocketCollector.Receiver(System.nanoTime(), true);
        stopped.closing = true;
        stopped.onOpen(socket);
        stopped.onBinary(socket, ByteBuffer.wrap(new byte[]{1}), true);
        stopped.onText(socket, "ignored", true);
        assertThat(stopped.failure).isNull();
        assertThat(stopped.length).isZero();
        assertThat(stopped.ready).isFalse();
        final WebSocketCollector.Receiver future = new WebSocketCollector.Receiver(System.nanoTime() + 60_000_000_000L, true);
        future.onBinary(socket, ByteBuffer.wrap(new byte[]{1}), true);
        assertThat(future.failure).hasMessageContaining("clock origin is in the future");
        assertThat(future.length).isZero();
        assertThat(future.ready).isFalse();
    }

    @Test void warmedTaggedBinaryAssemblyAndCaptureAllocateZeroWithEscapingNegativeControl() {
        final ThreadMXBean bean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        if (!bean.isThreadAllocatedMemoryEnabled()) { bean.setThreadAllocatedMemoryEnabled(true); }
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime(), true);
        receiver.onOpen(socket);
        final CaptureCodec codec = new CaptureCodec();
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final ByteBuffer direct = ByteBuffer.allocateDirect(32_752);
        direct.position(1).limit(32_751);
        final ByteBuffer first = direct.asReadOnlyBuffer();
        final ByteBuffer heap = ByteBuffer.allocate(WebSocketCollector.MAX_RAW_BYTES - 1 - first.remaining() + 2);
        heap.position(1).limit(heap.capacity() - 1);
        final ByteBuffer last = heap.slice().asReadOnlyBuffer();
        final ByteBuffer control = ByteBuffer.allocateDirect(0);
        binaryCycle(receiver, socket, codec, payload, first, last, control);
        assertThat(receiver.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(codec.validObservation(payload, 0, Wire.MAX_PAYLOAD_BYTES)).isTrue();
        assertThat(codec.rawLength).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(codec.buffer.getByte(codec.rawOffset)).isEqualTo((byte)1);
        for (int i = 0; i < 10_000; i++) { binaryCycle(receiver, socket, codec, payload, first, last, control); }
        final long small = measureBinary(bean, thread, receiver, socket, codec, payload, first, last, control, 256);
        final long large = measureBinary(bean, thread, receiver, socket, codec, payload, first, last, control, 512);
        assertThat(small).as("256 tagged maximum binary/text/capture cycles").isZero();
        assertThat(large).as("512 tagged maximum binary/text/capture cycles").isZero();
        assertThat(first.position()).isEqualTo(1);
        assertThat(first.limit()).isEqualTo(32_751);
        assertThat(last.position()).isZero();
        final long before = allocated(bean, thread);
        escaped = new byte[4096];
        assertThat(allocated(bean, thread) - before).as("escaping binary allocation counter control").isGreaterThanOrEqualTo(4096);
    }

    @Test void warmedMaximumAssemblyAndCaptureEncodingAllocateZeroWithEscapingNegativeControl() {
        final ThreadMXBean bean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        if (!bean.isThreadAllocatedMemoryEnabled()) { bean.setThreadAllocatedMemoryEnabled(true); }
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        final Socket socket = new Socket();
        final WebSocketCollector.Receiver receiver = new WebSocketCollector.Receiver(System.nanoTime());
        receiver.onOpen(socket);
        final CaptureCodec codec = new CaptureCodec();
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final String first = "x".repeat(WebSocketCollector.MAX_RAW_BYTES - 4) + "\uD83D";
        final String last = "\uDE00";
        cycle(receiver, socket, codec, payload, first, last);
        assertThat(receiver.length).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(codec.validObservation(payload, 0, Wire.MAX_PAYLOAD_BYTES)).isTrue();
        assertThat(codec.rawLength).isEqualTo(WebSocketCollector.MAX_RAW_BYTES);
        assertThat(codec.buffer.getByte(codec.rawOffset + codec.rawLength - 1)).isEqualTo((byte)0x80);
        for (int i = 0; i < 10_000; i++) { cycle(receiver, socket, codec, payload, first, last); }
        final long small = measure(bean, thread, receiver, socket, codec, payload, first, last, 256);
        final long large = measure(bean, thread, receiver, socket, codec, payload, first, last, 512);
        assertThat(small).as("256 maximum-size assembly/capture cycles").isZero();
        assertThat(large).as("512 maximum-size assembly/capture cycles").isZero();
        final long before = allocated(bean, thread);
        escaped = new byte[4096];
        assertThat(allocated(bean, thread) - before).as("escaping allocation counter control").isGreaterThanOrEqualTo(4096);
    }

    private static long measure(final ThreadMXBean bean, final long thread, final WebSocketCollector.Receiver receiver,
        final Socket socket, final CaptureCodec codec, final UnsafeBuffer payload, final String first, final String last,
        final int count) {
        final long before = allocated(bean, thread);
        for (int i = 0; i < count; i++) { cycle(receiver, socket, codec, payload, first, last); }
        return allocated(bean, thread) - before;
    }
    private static long allocated(final ThreadMXBean bean, final long thread) {
        final long bytes = bean.getThreadAllocatedBytes(thread);
        if (bytes < 0) { throw new AssertionError("allocation accounting unavailable"); }
        return bytes;
    }
    private static long measureBinary(final ThreadMXBean bean, final long thread, final WebSocketCollector.Receiver receiver,
        final Socket socket, final CaptureCodec codec, final UnsafeBuffer payload, final ByteBuffer first, final ByteBuffer last,
        final ByteBuffer control, final int count) {
        final long before = allocated(bean, thread);
        for (int i = 0; i < count; i++) { binaryCycle(receiver, socket, codec, payload, first, last, control); }
        return allocated(bean, thread) - before;
    }
    private static void binaryCycle(final WebSocketCollector.Receiver receiver, final Socket socket, final CaptureCodec codec,
        final UnsafeBuffer payload, final ByteBuffer first, final ByteBuffer last, final ByteBuffer control) {
        receiver.onText(socket, "{\"result\":null,\"id\":1}", true);
        if (!receiver.ready || receiver.failure != null) { throw new AssertionError("tagged text fixture did not complete"); }
        codec.encodeObservation(payload, 0, 1, 0, CaptureCodec.UNKNOWN_NATIVE_ID, null, 0, 0, receiver.raw, 0, receiver.length);
        receiver.release();
        receiver.onBinary(socket, first, false);
        receiver.onPing(socket, control);
        receiver.onBinary(socket, last, true);
        if (!receiver.ready || receiver.failure != null) { throw new AssertionError("binary fixture did not complete"); }
        codec.encodeObservation(payload, 0, 1, 0, CaptureCodec.UNKNOWN_NATIVE_ID, null, 0, 0, receiver.raw, 0, receiver.length);
        receiver.release();
    }
    private static byte[] tagged(final byte tag, final byte[] payload) {
        final byte[] bytes = new byte[payload.length + 1];
        bytes[0] = tag;
        System.arraycopy(payload, 0, bytes, 1, payload.length);
        return bytes;
    }
    private static void cycle(final WebSocketCollector.Receiver receiver, final Socket socket, final CaptureCodec codec,
                              final UnsafeBuffer payload, final String first, final String last) {
        receiver.onText(socket, first, false);
        receiver.onText(socket, last, true);
        if (!receiver.ready || receiver.failure != null) { throw new AssertionError("fixture did not complete"); }
        codec.encodeObservation(payload, 0, 1, 0, CaptureCodec.UNKNOWN_NATIVE_ID, null, 0, 0,
            receiver.raw, 0, receiver.length);
        receiver.release();
    }
    private static byte[] bytes(final WebSocketCollector.Receiver receiver) {
        final byte[] bytes = new byte[receiver.length];
        receiver.raw.getBytes(0, bytes);
        return bytes;
    }

    /** Demand accounting adds no allocation; network sends are deliberately outside the measured assembly path. */
    private static final class Socket implements WebSocket {
        long requests;
        boolean aborted;
        @Override public void request(final long n) { requests += n; }
        @Override public void abort() { aborted = true; }
        @Override public String getSubprotocol() { return ""; }
        @Override public boolean isOutputClosed() { return aborted; }
        @Override public boolean isInputClosed() { return aborted; }
        @Override public CompletableFuture<WebSocket> sendText(CharSequence text, boolean last) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<WebSocket> sendPing(ByteBuffer message) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<WebSocket> sendPong(ByteBuffer message) { throw new UnsupportedOperationException(); }
        @Override public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) { throw new UnsupportedOperationException(); }
    }
}
