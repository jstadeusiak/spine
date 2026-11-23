package spine.client;

import com.sun.management.ThreadMXBean;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;

class ClientAllocationTest {
    private static volatile Object escape;

    @Test void warmedMaximumApplicationReceiptAndConsumerDispatchAllocateZeroBytes() {
        final ThreadMXBean bean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        assertThat(bean.getThreadAllocatedBytes(thread)).isNotNegative();
        final Wire wire = new Wire();
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final UnsafeBuffer wrapped = new UnsafeBuffer(new byte[Wire.MAX_EVENT_PAYLOAD_BYTES]);
        final UnsafeBuffer event = new UnsafeBuffer(new byte[Wire.MAX_EVENT_BYTES]);
        final UnsafeBuffer receipt = new UnsafeBuffer(new byte[Wire.MAX_RESULT_BYTES]);
        wrapped.putShort(0, (short)(Wire.MAX_APPLICATION_PREFIX_BYTES - Wire.HEADER_LENGTH), Wire.ORDER);
        wrapped.putShort(2, (short)19, Wire.ORDER);
        wrapped.putShort(4, (short)777, Wire.ORDER);
        wrapped.putShort(6, (short)3, Wire.ORDER);
        wrapped.putInt(Wire.MAX_APPLICATION_PREFIX_BYTES, payload.capacity(), Wire.ORDER);
        wrapped.putBytes(Wire.APPLICATION_RESULT_OVERHEAD, payload, 0, payload.capacity());
        final ContiguousEventDispatcher dispatcher = new ContiguousEventDispatcher(11, 0);
        final EventHandler handler = (buffer, offset, length, decoded) -> { };
        try (ReliableProducer producer = new ReliableProducer(11, 17, 1)) {
            for (int i = 0; i < 30000; i++) { applicationCycle(wire, producer, dispatcher, handler, payload, wrapped, event, receipt); }
            final long before = bean.getThreadAllocatedBytes(thread);
            for (int i = 0; i < 20000; i++) { applicationCycle(wire, producer, dispatcher, handler, payload, wrapped, event, receipt); }
            final long first = bean.getThreadAllocatedBytes(thread) - before;
            final long again = bean.getThreadAllocatedBytes(thread);
            for (int i = 0; i < 40000; i++) { applicationCycle(wire, producer, dispatcher, handler, payload, wrapped, event, receipt); }
            final long second = bean.getThreadAllocatedBytes(thread) - again;
            assertThat(first).isZero();
            assertThat(second).isZero();
        }
        final long before = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < 1000; i++) { escape = new byte[128]; }
        assertThat(bean.getThreadAllocatedBytes(thread) - before).isGreaterThanOrEqualTo(128_000);
    }

    private static void applicationCycle(final Wire wire, final ReliableProducer producer,
        final ContiguousEventDispatcher dispatcher, final EventHandler handler, final UnsafeBuffer payload,
        final UnsafeBuffer wrapped, final UnsafeBuffer event, final UnsafeBuffer receipt) {
        final long sequence = producer.nextSequence();
        if (!producer.submit(1, 1, 1, 100, 101, 102, payload, 0, payload.capacity())) {
            throw new AssertionError("proposal refused");
        }
        final int eventLength = wire.encodeEvent(event, 0, 11, sequence, 200, 1, 1, 1,
            100, 101, 102, wrapped, 0, wrapped.capacity());
        final int length = wire.encodeResult(receipt, 0, 17, sequence, AdmissionStatus.ACCEPTED, event, 0, eventLength);
        producer.onReceipt(receipt, 0, length);
        if (producer.pendingLength() != 0) { throw new AssertionError("application receipt refused"); }
        dispatcher.onMessage(event, 0, eventLength, handler, false);
    }

    @Test void warmedMaximumPayloadEncodeReceiptAndDispatchHaveNoAllocationSlope() {
        final ThreadMXBean bean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        assertThat(bean.getThreadAllocatedBytes(thread)).isNotNegative();
        final Wire wire = new Wire();
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final UnsafeBuffer event = new UnsafeBuffer(new byte[Wire.MAX_EVENT_BYTES]);
        final UnsafeBuffer receipt = new UnsafeBuffer(new byte[Wire.MAX_RESULT_BYTES]);
        final ContiguousEventDispatcher dispatcher = new ContiguousEventDispatcher(11, 0);
        final EventHandler handler = (buffer, offset, length, decoded) -> { };
        try (ReliableProducer producer = new ReliableProducer(11, 17, 1)) {
            for (int i = 0; i < 6000; i++) { cycle(wire, producer, dispatcher, handler, payload, event, receipt); }
            final long before = bean.getThreadAllocatedBytes(thread);
            for (int i = 0; i < 2000; i++) { cycle(wire, producer, dispatcher, handler, payload, event, receipt); }
            final long first = bean.getThreadAllocatedBytes(thread) - before;
            final long again = bean.getThreadAllocatedBytes(thread);
            for (int i = 0; i < 4000; i++) { cycle(wire, producer, dispatcher, handler, payload, event, receipt); }
            final long second = bean.getThreadAllocatedBytes(thread) - again;
            // Fixed JIT/accounting noise is separately bounded; doubling events cannot permit proportional garbage.
            assertThat(first).isLessThanOrEqualTo(4096);
            assertThat(second).isLessThanOrEqualTo(4096);
        }
        try (ReliableProducer malformed = new ReliableProducer(11, 17, 1)) {
            final UnsafeBuffer bad = new UnsafeBuffer(new byte[8]);
            bad.putShort(0, (short)1, Wire.ORDER);
            for (int i = 0; i < 10000; i++) { refuse(malformed, bad); }
            final long malformedStart = bean.getThreadAllocatedBytes(thread);
            for (int i = 0; i < 20000; i++) { refuse(malformed, bad); }
            assertThat(bean.getThreadAllocatedBytes(thread) - malformedStart).isLessThanOrEqualTo(4096);
        }
        final long before = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < 1000; i++) { escape = new byte[128]; }
        assertThat(bean.getThreadAllocatedBytes(thread) - before).isGreaterThanOrEqualTo(128_000);
    }

    private static void refuse(final ReliableProducer producer, final UnsafeBuffer bad) {
        if (producer.submit(1, 1, 1, -1, -1, -1, bad, 0, 8)
            || producer.submit(1, 1, 1, -1, -1, -1, null, 0, 8)) { throw new AssertionError("malformed admitted"); }
    }

    private static void cycle(final Wire wire, final ReliableProducer producer,
                              final ContiguousEventDispatcher dispatcher, final EventHandler handler,
                              final UnsafeBuffer payload, final UnsafeBuffer event, final UnsafeBuffer receipt) {
        final long sequence = producer.nextSequence();
        if (!producer.submit(Integer.MIN_VALUE, 2, 3, 100, 101, 102, payload, 0, payload.capacity())) {
            throw new AssertionError("proposal refused");
        }
        final int eventLength = wire.encodeEvent(event, 0, 11, sequence, 200, Integer.MIN_VALUE, 2, 3,
            100, 101, 102, payload, 0, payload.capacity());
        final int receiptLength = wire.encodeResult(receipt, 0, 17, sequence, AdmissionStatus.ACCEPTED,
            event, 0, eventLength);
        producer.onReceipt(receipt, 0, receiptLength);
        if (producer.pendingLength() != 0) { throw new AssertionError("receipt refused"); }
        dispatcher.onMessage(event, 0, eventLength, handler, false);
        dispatcher.onMessage(event, 0, eventLength, handler, false);
    }
}
