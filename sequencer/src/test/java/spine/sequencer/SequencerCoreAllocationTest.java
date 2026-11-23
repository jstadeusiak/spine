package spine.sequencer;

import com.sun.management.ThreadMXBean;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

class SequencerCoreAllocationTest {
    private static volatile byte[] escapingAllocation;
    private static volatile long consumed;
    private final ThreadMXBean bean = allocationAccounting();
    private final long threadId = Thread.currentThread().threadId();
    private final Wire wire = new Wire();
    private long producerSequence;

    @Test
    void realDecodeMutationEncodingAndSnapshotHaveNoWarmedAllocationSlope() {
        final SequencerCore core = initialized();
        final UnsafeBuffer input = proposal(11, 21, 1, 16);
        final Runnable accepted = () -> {
            input.putLong(24, ++producerSequence, ByteOrder.LITTLE_ENDIAN);
            require(core.onCommand(input, 0, input.capacity(), producerSequence, 1), AdmissionStatus.ACCEPTED);
            consumed ^= core.resultBuffer().getLong(core.eventOffset() + 16, ByteOrder.LITTLE_ENDIAN);
        };
        proveNoAllocation("accepted 16-byte payload", accepted, 100_000, 10_000, 20_000);
        proveNoAllocation("snapshot encode", () -> consumed ^= core.snapshot(), 30_000, 10_000, 20_000);
        final UnsafeBuffer conflict = new UnsafeBuffer(input.byteArray().clone());
        conflict.putLong(80, 999, ByteOrder.LITTLE_ENDIAN);
        final UnsafeBuffer gap = new UnsafeBuffer(input.byteArray().clone());
        gap.putLong(24, producerSequence + 2, ByteOrder.LITTLE_ENDIAN);
        final UnsafeBuffer expired = new UnsafeBuffer(input.byteArray().clone());
        expired.putLong(24, producerSequence - 1, ByteOrder.LITTLE_ENDIAN);
        final UnsafeBuffer wrongLog = new UnsafeBuffer(input.byteArray().clone());
        wrongLog.putLong(8, 12, ByteOrder.LITTLE_ENDIAN);
        final UnsafeBuffer malformed = new UnsafeBuffer(input.byteArray().clone());
        malformed.putInt(68, -1, ByteOrder.LITTLE_ENDIAN);
        final Runnable rejectionsAndRetry = () -> {
            require(core.onCommand(input, 0, input.capacity(), 100, 1), AdmissionStatus.DUPLICATE);
            require(core.onCommand(conflict, 0, conflict.capacity(), 100, 1), AdmissionStatus.CONFLICT);
            require(core.onCommand(gap, 0, gap.capacity(), 100, 1), AdmissionStatus.GAP);
            require(core.onCommand(expired, 0, expired.capacity(), 100, 1), AdmissionStatus.EXPIRED);
            require(core.onCommand(wrongLog, 0, wrongLog.capacity(), 100, 1), AdmissionStatus.LOG_CONFLICT);
            require(core.onCommand(malformed, 0, malformed.capacity(), 100, 1), AdmissionStatus.INVALID);
            require(core.onCommand(input, -1, input.capacity(), 100, 1), AdmissionStatus.INVALID);
            consumed ^= core.resultLength();
        };
        proveNoAllocation("retry/conflict/gap/expired/log/malformed", rejectionsAndRetry, 100_000, 10_000, 20_000);
        proveNegativeControl();
    }

    @Test
    void maximumPayloadAdmissionRetryAndMaximumSnapshotUseOwnedPreallocatedStorage() {
        final SequencerCore core = initialized();
        final UnsafeBuffer input = proposal(11, 21, 1, Wire.MAX_PAYLOAD_BYTES);
        final Runnable accepted = () -> {
            input.putLong(24, ++producerSequence, ByteOrder.LITTLE_ENDIAN);
            require(core.onCommand(input, 0, input.capacity(), producerSequence, 1), AdmissionStatus.ACCEPTED);
            consumed ^= core.resultBuffer().getLong(core.eventOffset() + 16, ByteOrder.LITTLE_ENDIAN);
        };
        proveNoAllocation("accepted 64-KiB payload", accepted, 32_768, 2_048, 4_096);
        proveNoAllocation("duplicate 64-KiB payload", () -> {
            require(core.onCommand(input, 0, input.capacity(), 0, 1), AdmissionStatus.DUPLICATE);
            consumed ^= core.resultBuffer().getLong(Wire.RESULT_OVERHEAD + 16, ByteOrder.LITTLE_ENDIAN);
        }, 32_768, 2_048, 4_096);
        final UnsafeBuffer nextProducer = proposal(11, 22, 1, 16);
        for (int i = 1; i < Wire.MAX_PRODUCERS; i++) {
            nextProducer.putLong(16, 21 + i, ByteOrder.LITTLE_ENDIAN);
            require(core.onCommand(nextProducer, 0, nextProducer.capacity(), i, 1), AdmissionStatus.ACCEPTED);
        }
        nextProducer.putLong(16, 10_000, ByteOrder.LITTLE_ENDIAN);
        proveNoAllocation("capacity rejection", () -> {
            require(core.onCommand(nextProducer, 0, nextProducer.capacity(), 1, 1), AdmissionStatus.CAPACITY);
            consumed ^= core.resultLength();
        }, 30_000, 10_000, 20_000);
        proveNoAllocation("maximum producer snapshot encode", () -> consumed ^= core.snapshot(), 2_048, 1_024, 2_048);
    }

    @Test
    void emptyPollingStyleMalformedAndUninitializedDecodingAlsoAllocateNothing() {
        final SequencerCore core = new SequencerCore();
        final UnsafeBuffer input = proposal(11, 21, 1, 16);
        final Runnable reject = () -> {
            require(core.onCommand(input, 0, input.capacity(), 1, 1), AdmissionStatus.UNINITIALIZED);
            require(core.onCommand(input, 0, 7, 1, 1), AdmissionStatus.INVALID);
            require(core.onCommand(null, 0, 0, 1, 1), AdmissionStatus.INVALID);
            consumed ^= core.resultLength();
        };
        proveNoAllocation("uninitialized/malformed", reject, 100_000, 10_000, 20_000);
    }

    private void proveNoAllocation(final String path, final Runnable action, final int warmup,
        final int shortRun, final int longRun) {
        for (int i = 0; i < warmup; i++) {
            action.run();
        }
        // Warm the measurement method and MXBean reads before taking the two independent windows.
        allocated(action, 32);
        final long small = allocated(action, shortRun);
        final long large = allocated(action, longRun);
        assertThat(small).as("%s: short=%s bytes/%s ops; long=%s bytes/%s ops", path, small, shortRun, large, longRun).isZero();
        assertThat(large).as("%s: %s operations", path, longRun).isZero();
    }

    private long allocated(final Runnable action, final int operations) {
        final long before = bean.getThreadAllocatedBytes(threadId);
        for (int i = 0; i < operations; i++) {
            action.run();
        }
        final long after = bean.getThreadAllocatedBytes(threadId);
        if (before < 0 || after < before) {
            throw new AssertionError("allocation accounting stopped or decreased");
        }
        return after - before;
    }

    private void proveNegativeControl() {
        final long bytes = allocated(() -> escapingAllocation = new byte[64], 1_024);
        assertThat(bytes).as("escaping allocation must be visible to the accounting gate").isGreaterThanOrEqualTo(65_536);
        escapingAllocation = null;
    }

    private SequencerCore initialized() {
        final SequencerCore core = new SequencerCore();
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        wire.encodeInitialize(input, 0, 11);
        require(core.onCommand(input, 0, input.capacity(), 0, 1), AdmissionStatus.INITIALIZED);
        return core;
    }

    private UnsafeBuffer proposal(final long log, final long producer, final long sequence, final int payloadLength) {
        final UnsafeBuffer payload = SequencerCoreTest.payload(7, payloadLength);
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payloadLength]);
        wire.encodeProposal(input, 0, log, producer, sequence, 1, 2, 3, 10, 11, 12, payload, 0, payloadLength);
        return input;
    }

    private static void require(final AdmissionStatus actual, final AdmissionStatus expected) {
        if (actual != expected) {
            throw new AssertionError("unexpected admission status: " + actual);
        }
    }

    private static ThreadMXBean allocationAccounting() {
        final java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assertThat(management).as("HotSpot thread allocation accounting").isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean)management;
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        if (!bean.isThreadAllocatedMemoryEnabled()) {
            bean.setThreadAllocatedMemoryEnabled(true);
        }
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        assertThat(bean.getThreadAllocatedBytes(Thread.currentThread().threadId())).isGreaterThanOrEqualTo(0);
        return bean;
    }
}
