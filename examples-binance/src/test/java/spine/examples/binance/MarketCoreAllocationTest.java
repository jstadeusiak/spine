package spine.examples.binance;

import spine.sequencer.SequencerCore;

import com.sun.management.ThreadMXBean;
import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class MarketCoreAllocationTest {
    private static volatile byte[] escaping;
    private static volatile long consumed;
    private final ThreadMXBean bean = accounting();
    private final long thread = Thread.currentThread().threadId();

    @Test
    void realJsonAndSbeBookMutationRedundancyProducerRetriesGapRepairAndSnapshotAllocateNothing() {
        final Cycle cycle = new Cycle();
        prove("JSON/SBE mutation, redundant update, retry after later market mutation, gap repair", cycle::run,
            100_000, 10_000, 20_000);
        prove("complete core/market/receipt snapshot encoding", () -> consumed ^= cycle.core.snapshot(),
            30_000, 10_000, 20_000);
        negativeControl();
    }

    @Test
    void malformedBoundCaptureAndKnownSourceScopeRejectionsHaveNoAllocationSlope() {
        final MarketCoreTest.Inputs inputs = new MarketCoreTest.Inputs();
        final SequencerCore core = MarketCoreTest.ready(inputs);
        final UnsafeBuffer malformed = inputs.observation(1, 1, 1,
            MarketCoreTest.utf8(MarketCoreTest.diff(101, 101, "[[\"100\",\"-1\"]]", "[]")));
        final long[] sequence = {inputs.submitted[1] - 1};
        final Runnable reject = () -> {
            malformed.putLong(24, ++sequence[0], Wire.ORDER);
            malformed.putLong(Wire.PROPOSAL_OVERHEAD + 8, sequence[0], Wire.ORDER);
            require(core.onCommand(malformed, 0, malformed.capacity(), sequence[0] + 2000, 1), AdmissionStatus.ACCEPTED);
            require(((BinanceApplication)core.application()).book.status == BinanceBook.INVALID);
            require(((BinanceApplication)core.application()).book.nativeFrontier == 100);
            consumed ^= core.resultLength();
        };
        prove("malformed admitted bound capture", reject, 100_000, 10_000, 20_000);
        malformed.putInt(36, 2, Wire.ORDER); // Existing source, unmatched configuration.
        prove("known source configuration mismatch", reject, 100_000, 10_000, 20_000);
        negativeControl();
    }

    @Test
    void maximumBootstrapFragmentAndItsInProgressSnapshotUseOnlyOwnedPreallocatedStorage() {
        final MarketCoreTest.Inputs inputs = new MarketCoreTest.Inputs();
        final SequencerCore core = MarketCoreTest.ready(inputs);
        final byte[] bytes = new byte[Wire.MAX_PAYLOAD_BYTES - 28];
        Arrays.fill(bytes, (byte)' ');
        bytes[0] = '{';
        final UnsafeBuffer chunk = inputs.chunk(3, 1, 1, 2, 512 * 1024, 0, bytes);
        final long[] submitted = {inputs.submitted[3] - 1}, bootstrap = {1};
        final Runnable staged = () -> {
            chunk.putLong(24, ++submitted[0], Wire.ORDER);
            chunk.putLong(Wire.PROPOSAL_OVERHEAD + 8, ++bootstrap[0], Wire.ORDER);
            require(core.onCommand(chunk, 0, chunk.capacity(), submitted[0] + 2000, 1), AdmissionStatus.ACCEPTED);
            require(((BinanceApplication)core.application()).book.nativeFrontier == 100);
            require(core.eventLength() == core.limits().eventBytes());
            consumed ^= core.resultBuffer().getLong(core.eventOffset() + Wire.EVENT_OVERHEAD + 24, Wire.ORDER);
        };
        prove("maximum bootstrap fragment result", staged, 32_768, 2048, 4096);
        prove("snapshot with maximum staged fragment bytes", () -> consumed ^= core.snapshot(), 30_000, 1024, 2048);
        negativeControl();
    }

    @Test
    void maximumBookSnapshotEncodesBothSidesWithoutAllocatingSortingOrGroupWorkspace() {
        final MarketCoreTest.Inputs inputs = new MarketCoreTest.Inputs();
        final SequencerCore core = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        MarketCoreTest.initialize(core);
        MarketCoreTest.send(core, inputs.bind(3, 3, 1, 1, 1), 1000);
        final StringBuilder json = new StringBuilder("{\"lastUpdateId\":100,\"bids\":[");
        for (int side = 0; side < 2; side++) {
            if (side != 0) { json.append("],\"asks\":["); }
            for (int i = 0; i < BinanceBook.MAX_LEVELS; i++) {
                if (i != 0) { json.append(','); }
                json.append("[\"").append((side + 1) * 100_000 + i).append("\",\"1\"]");
            }
        }
        json.append("]}");
        final byte[] raw = MarketCoreTest.utf8(json.toString());
        assertThat(raw.length).isLessThanOrEqualTo(BinanceApplication.MAX_BOOTSTRAP_BYTES);
        for (int offset = 0; offset < raw.length;) {
            final int size = Math.min(Wire.MAX_PAYLOAD_BYTES - 28, raw.length - offset);
            MarketCoreTest.send(core, inputs.chunk(3, 1, 1, 1, raw.length, offset,
                Arrays.copyOfRange(raw, offset, offset + size)), 2000 + offset);
            offset += size;
        }
        assertThat(((BinanceApplication)core.application()).book.bids.size()).isEqualTo(BinanceBook.MAX_LEVELS);
        assertThat(((BinanceApplication)core.application()).book.asks.size()).isEqualTo(BinanceBook.MAX_LEVELS);
        prove("maximum complete book snapshot", () -> consumed ^= core.snapshot(), 2048, 1024, 2048);
        negativeControl();
    }

    private void prove(final String path, final Runnable action, final int warmup, final int smallCount, final int largeCount) {
        for (int i = 0; i < warmup; i++) { action.run(); }
        allocated(action, 32);
        final long small = allocated(action, smallCount), large = allocated(action, largeCount);
        assertThat(small).as("%s: %s operations", path, smallCount).isZero();
        assertThat(large).as("%s: %s operations", path, largeCount).isZero();
    }

    private long allocated(final Runnable action, final int count) {
        final long before = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < count; i++) { action.run(); }
        final long after = bean.getThreadAllocatedBytes(thread);
        require(before >= 0 && after >= before);
        return after - before;
    }

    private void negativeControl() {
        assertThat(allocated(() -> escaping = new byte[64], 1024)).isGreaterThanOrEqualTo(65_536);
        escaping = null;
    }

    private static ThreadMXBean accounting() {
        final java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assertThat(management).isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean)management;
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        return bean;
    }

    private static void require(final AdmissionStatus actual, final AdmissionStatus expected) {
        if (actual != expected) { throw new AssertionError("unexpected market admission status"); }
    }

    private static void require(final boolean valid) { if (!valid) { throw new AssertionError("market allocation fixture failed"); } }

    private static final class Cycle {
        final MarketCoreTest.Inputs inputs = new MarketCoreTest.Inputs();
        final SequencerCore core = MarketCoreTest.ready(inputs);
        final UnsafeBuffer json, binary;
        final int firstOffset, lastOffset;
        long nativeId = 999_999_999, jsonSequence, sbeSequence, time = 2001;

        Cycle() {
            MarketCoreTest.bootstrap(core, inputs, 2, MarketCoreTest.SNAPSHOT.replace(":100,", ":999999999,"), 2000);
            final String text = MarketCoreTest.diff(1_000_000_000, 1_000_000_000, MarketCoreTest.oneBid("1"), "[]");
            json = inputs.observation(1, 1, 1, MarketCoreTest.utf8(text));
            binary = inputs.observation(2, 1, 1, MarketCoreTest.sbe(1_000_000_000, 1_000_000_000, 1));
            jsonSequence = inputs.submitted[1] - 1;
            sbeSequence = inputs.submitted[2] - 1;
            firstOffset = Wire.PROPOSAL_OVERHEAD + CaptureCodec.OBSERVATION_OVERHEAD + text.indexOf("\"U\":") + 4;
            lastOffset = Wire.PROPOSAL_OVERHEAD + CaptureCodec.OBSERVATION_OVERHEAD + text.indexOf("\"u\":") + 4;
        }

        void run() {
            json(++nativeId);
            require(((BinanceApplication)core.application()).book.nativeFrontier == nativeId);
            binary(nativeId, 1); // Same native update, separately retained provider observation.
            require(((BinanceApplication)core.application()).book.nativeFrontier == nativeId);
            binary(++nativeId, 2);
            require(((BinanceApplication)core.application()).book.nativeFrontier == nativeId);
            require(core.onCommand(json, 0, json.capacity(), ++time, 1), AdmissionStatus.DUPLICATE);
            require(((BinanceApplication)core.application()).book.nativeFrontier == nativeId);
            consumed ^= core.resultBuffer().getLong(core.eventOffset() + Wire.EVENT_OVERHEAD + 24, Wire.ORDER);
            json(nativeId + 2);
            require(((BinanceApplication)core.application()).book.status == BinanceBook.GAP);
            binary(nativeId + 1, 1);
            nativeId += 2;
            require(((BinanceApplication)core.application()).book.nativeFrontier == nativeId);
            require(((BinanceApplication)core.application()).book.pendingCount == 0 && ((BinanceApplication)core.application()).book.eligible());
            require(core.onCommand(binary, 0, binary.capacity(), ++time, 1), AdmissionStatus.DUPLICATE);
            consumed ^= core.resultLength();
        }

        private void json(final long id) {
            json.putLong(24, ++jsonSequence, Wire.ORDER);
            json.putLong(Wire.PROPOSAL_OVERHEAD + 8, jsonSequence - 1, Wire.ORDER);
            decimal(json, firstOffset, id);
            decimal(json, lastOffset, id);
            require(core.onCommand(json, 0, json.capacity(), ++time, 1), AdmissionStatus.ACCEPTED);
        }

        private void binary(final long id, final long size) {
            binary.putLong(24, ++sbeSequence, Wire.ORDER);
            binary.putLong(Wire.PROPOSAL_OVERHEAD + 8, sbeSequence - 1, Wire.ORDER);
            final int raw = Wire.PROPOSAL_OVERHEAD + CaptureCodec.OBSERVATION_OVERHEAD + 1;
            binary.putLong(raw + 16, id, Wire.ORDER);
            binary.putLong(raw + 24, id, Wire.ORDER);
            binary.putLong(raw + 46, size, Wire.ORDER);
            require(core.onCommand(binary, 0, binary.capacity(), ++time, 1), AdmissionStatus.ACCEPTED);
        }

        private static void decimal(final UnsafeBuffer buffer, final int offset, final long id) {
            long value = id;
            for (int i = 9; i >= 0; i--) {
                buffer.putByte(offset + i, (byte)('0' + value % 10));
                value /= 10;
            }
            require(value == 0);
        }
    }
}
