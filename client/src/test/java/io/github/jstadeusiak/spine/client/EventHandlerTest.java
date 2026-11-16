package io.github.jstadeusiak.spine.client;

import com.sun.management.ThreadMXBean;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.*;

class EventHandlerTest {
    private static volatile Object escaped;
    private static volatile long observed;
    private final Wire wire = new Wire();
    private final UnsafeBuffer payload = new UnsafeBuffer(new byte[32]);
    private final UnsafeBuffer event = new UnsafeBuffer(new byte[Wire.EVENT_OVERHEAD + 32 + 11]);
    private final int offset = 11;

    @Test void twoHandlersEachConsumeTheWholeBorrowedPayloadFromTheStart() {
        final UnsafeBuffer first = new UnsafeBuffer(new byte[32]);
        final UnsafeBuffer second = new UnsafeBuffer(new byte[32]);
        final int[] calls = {0};
        final int length = encode();
        final EventHandler chain = reader(first, calls).andThen(reader(second, calls));
        chain.onEvent(event, offset, length, wire.event);
        assertThat(calls[0]).isEqualTo(2);
        assertThat(first.byteArray()).containsExactly(payload.byteArray());
        assertThat(second.byteArray()).containsExactly(payload.byteArray());
        assertThat(wire.event.limit()).isEqualTo(offset + length);
    }

    @Test void nestedThreeHandlerCompositionRestartsAnAlreadyConsumedCursorAndPreservesOrder() {
        final UnsafeBuffer[] copies = {new UnsafeBuffer(new byte[32]), new UnsafeBuffer(new byte[32]), new UnsafeBuffer(new byte[32])};
        final int[] calls = {0};
        final int[] order = new int[3];
        final EventHandler[] handlers = new EventHandler[3];
        for (int i = 0; i < handlers.length; i++) {
            final int index = i;
            handlers[i] = (buffer, start, length, decoded) -> {
                order[calls[0]++] = index;
                assertThat(buffer).isSameAs(event);
                assertThat(start).isEqualTo(offset);
                assertThat(decoded.sequence()).isEqualTo(1);
                assertThat(decoded.payloadLength()).isEqualTo(32);
                assertThat(decoded.getPayload(copies[index], 0, 32)).isEqualTo(32);
            };
        }
        final int length = encode();
        wire.event.skipPayload();
        handlers[0].andThen(handlers[1]).andThen(handlers[2]).onEvent(event, offset, length, wire.event);
        assertThat(order).containsExactly(0, 1, 2);
        calls[0] = 0;
        handlers[0].andThen(handlers[1].andThen(handlers[2])).onEvent(event, offset, length, wire.event);
        assertThat(order).containsExactly(0, 1, 2);
        for (final UnsafeBuffer copy : copies) assertThat(copy.byteArray()).containsExactly(payload.byteArray());
    }

    @Test void nullHandlerIsRejectedAtCompositionBeforeAnyCallback() {
        final EventHandler handler = (buffer, start, length, decoded) -> { throw new AssertionError("called during setup"); };
        assertThatNullPointerException().isThrownBy(() -> handler.andThen(null)).withMessage("next");
    }

    @Test void firstFailurePropagatesUnchangedAndPreventsLaterHandlers() {
        final RuntimeException failure = new IllegalArgumentException("first failed");
        final int[] calls = {0};
        final EventHandler first = (buffer, start, length, decoded) -> { calls[0]++; decoded.skipPayload(); throw failure; };
        final EventHandler later = (buffer, start, length, decoded) -> { throw new AssertionError("later handler called"); };
        final int length = encode();
        assertThatThrownBy(() -> first.andThen(later).andThen(later).onEvent(event, offset, length, wire.event)).isSameAs(failure);
        assertThat(calls[0]).isEqualTo(1);
    }

    @Test void laterFailureKeepsTheApplicationCursorButDoesNotRollbackEarlierEffects() {
        final ContiguousEventDispatcher dispatcher = new ContiguousEventDispatcher(7, 0);
        final RuntimeException failure = new IllegalArgumentException("second failed");
        final int[] effects = {0};
        final EventHandler first = (buffer, start, length, decoded) -> { decoded.skipPayload(); effects[0]++; };
        final EventHandler second = (buffer, start, length, decoded) -> {
            assertThat(decoded.payloadLength()).isEqualTo(32);
            decoded.skipPayload();
            throw failure;
        };
        final EventHandler later = (buffer, start, length, decoded) -> { throw new AssertionError("third handler called"); };
        final EventHandler chain = first.andThen(second).andThen(later);
        final int length = encode();
        assertThatThrownBy(() -> dispatcher.onMessage(event, offset, length, chain, false)).isSameAs(failure);
        assertThat(effects[0]).isEqualTo(1);
        assertThat(dispatcher.sequence()).isZero();
    }

    @Test void warmedComposedPayloadCallbacksAllocateExactlyZeroBytes() {
        assertThat(ManagementFactory.getThreadMXBean()).isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        final UnsafeBuffer[] copies = {new UnsafeBuffer(new byte[32]), new UnsafeBuffer(new byte[32]), new UnsafeBuffer(new byte[32])};
        final long[] sum = {0};
        final EventHandler first = allocationReader(copies[0], sum);
        final EventHandler chain = first.andThen(allocationReader(copies[1], sum)).andThen(allocationReader(copies[2], sum));
        final int length = encode();
        exercise(chain, length, 100000);
        allocated(bean, thread, chain, length, 1000);
        final long small = allocated(bean, thread, chain, length, 10000);
        final long large = allocated(bean, thread, chain, length, 100000);
        assertThat(small).isZero();
        assertThat(large).isZero();
        assertThat(sum[0]).isEqualTo(3L * (100000 + 1000 + 10000 + 100000));
        for (final UnsafeBuffer copy : copies) assertThat(copy.byteArray()).containsExactly(payload.byteArray());
        final long beforeControl = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < 1000; i++) escaped = new byte[128];
        assertThat(bean.getThreadAllocatedBytes(thread) - beforeControl).isGreaterThanOrEqualTo(128000);
        observed = sum[0];
    }

    private EventHandler reader(final UnsafeBuffer copy, final int[] calls) {
        return (buffer, start, length, decoded) -> {
            calls[0]++;
            assertThat(decoded.payloadLength()).isEqualTo(32);
            assertThat(decoded.getPayload(copy, 0, 32)).isEqualTo(32);
        };
    }

    private static EventHandler allocationReader(final UnsafeBuffer copy, final long[] sum) {
        return (buffer, start, length, decoded) -> {
            if (decoded.payloadLength() != 32 || decoded.getPayload(copy, 0, 32) != 32) throw new AssertionError("incomplete payload");
            sum[0] += decoded.sequence();
        };
    }

    private long allocated(final ThreadMXBean bean, final long thread, final EventHandler chain, final int length, final int count) {
        final long before = bean.getThreadAllocatedBytes(thread);
        exercise(chain, length, count);
        final long after = bean.getThreadAllocatedBytes(thread);
        if (before < 0 || after < before) throw new AssertionError("invalid allocation accounting");
        return after - before;
    }

    private void exercise(final EventHandler chain, final int length, final int count) {
        for (int i = 0; i < count; i++) chain.onEvent(event, offset, length, wire.event);
    }

    private int encode() {
        payload.putShort(0, (short)0, Wire.ORDER);
        payload.putShort(2, (short)1, Wire.ORDER);
        payload.putShort(4, (short)301, Wire.ORDER);
        for (int i = 8; i < payload.capacity(); i++) payload.putByte(i, (byte)(i + 3));
        final int length = wire.encodeEvent(event, offset, 7, 1, 123, 1, 1, 1, 100, 101, 102,
            payload, 0, payload.capacity());
        assertThat(wire.validEvent(event, offset, length)).isTrue();
        return length;
    }
}
