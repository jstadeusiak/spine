package spine.testkit;

import com.sun.management.ThreadMXBean;
import spine.protocol.ApplicationIdentity;
import spine.protocol.ApplicationLimits;
import spine.protocol.Wire;
import spine.protocol.sbe.ObservationProposalDecoder;
import spine.sequencer.Application;
import spine.testkit.counter.sbe.CounterBatchResultDecoder;
import spine.testkit.counter.sbe.CounterBatchResultEncoder;
import spine.testkit.counter.sbe.MessageHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.io.DataInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApplicationConformanceTest {
    private static volatile Object escape;

    @Test void neutralApplicationConformsAcrossEveryCutAndRetry() throws Exception {
        final ApplicationConformance.Report report = ApplicationConformance.verify(
            CounterApplication::new, CounterApplication.IDENTITY, CounterExample.LOG_ID, CounterExample.corpus());
        assertThat(report.commands()).isEqualTo(9);
        assertThat(report.snapshotCuts()).isEqualTo(10);
        assertThat(report.canonicalSha256()).isEqualTo("02a96529abded055c7448fe0efc71fe6e9e64d1c2b00fceef371ad3387913f58");
        assertThat(report.finalSnapshotSha256()).isEqualTo("cce5529d571e1c3ca62cd15bb9221f954d8faeee2d9ad617c02b60ba5d550b64");
        final MessageDigest sha = MessageDigest.getInstance("SHA-256");
        try (var canonical = getClass().getResourceAsStream("/counter-oracle/canonical.bin")) {
            assertThat(HexFormat.of().formatHex(sha.digest(canonical.readAllBytes()))).isEqualTo(report.canonicalSha256());
        }
        try (var snapshot = getClass().getResourceAsStream("/counter-oracle/final-snapshot.bin")) {
            final byte[] bytes = snapshot.readAllBytes();
            for (int shift = 24; shift >= 0; shift -= 8) { sha.update((byte)(bytes.length >>> shift)); }
            assertThat(HexFormat.of().formatHex(sha.digest(bytes))).isEqualTo(report.finalSnapshotSha256());
        }
        try (var commands = new DataInputStream(getClass().getResourceAsStream("/counter-oracle/commands-and-time.bin"))) {
            for (final ApplicationConformance.Command command : CounterExample.corpus()) {
                assertThat(commands.readLong()).isEqualTo(command.loggedTimeNanos());
                assertThat(commands.readNBytes(commands.readInt())).isEqualTo(command.bytes());
            }
            assertThat(commands.read()).isEqualTo(-1);
        }
    }

    @Test void variableApplicationGroupsConformWithDeclaredResultAndSnapshotBounds() {
        final ApplicationConformance.Report report = ApplicationConformance.verify(BatchApplication::new,
            new ApplicationIdentity(502, 1, 1), new ApplicationLimits(2, 512, 16),
            CounterExample.LOG_ID, CounterExample.corpus());
        assertThat(report.commands()).isEqualTo(9);
        assertThat(report.snapshotCuts()).isEqualTo(10);
    }

    @Test void counterHasLiteralIndependentCommandResultAndStateBytes() {
        final CounterApplication application = new CounterApplication();
        final UnsafeBuffer input = hex("08000100f50101000300000000000000");
        final UnsafeBuffer result = new UnsafeBuffer(new byte[ApplicationLimits.DEFAULT.resultBytes()]);
        assertThat(application.apply(input, 0, input.capacity(), null, 100, result, 0)).isEqualTo(25);
        assertThat(bytes(result, 25)).isEqualTo(hex("11000200f50101000300000000000000640000000000000001").byteArray());
        assertThat(application.snapshot(result, 0)).isEqualTo(16);
        assertThat(bytes(result, 16)).isEqualTo(hex("08000300f50101000300000000000000").byteArray());
        assertThat(application.restore(result, 0, 16, 1).total()).isEqualTo(3);
        final UnsafeBuffer negative = hex("08000100f5010100ffffffffffffffff");
        assertThat(application.apply(negative, 0, negative.capacity(), null, 101, result, 0)).isEqualTo(25);
        assertThat(bytes(result, 25)).isEqualTo(hex("11000200f50101000300000000000000650000000000000000").byteArray());
        assertThat(application.total()).isEqualTo(3);
    }

    @Test void conformanceDetectsNondeterminismAndBrokenRestore() {
        final AtomicInteger generation = new AtomicInteger();
        assertThatThrownBy(() -> ApplicationConformance.verify(() -> new BrokenApplication(generation.incrementAndGet(), false),
            CounterApplication.IDENTITY, CounterExample.LOG_ID, CounterExample.corpus()))
            .isInstanceOf(AssertionError.class).hasMessageContaining("repeated receipts");
        assertThatThrownBy(() -> ApplicationConformance.verify(() -> new BrokenApplication(0, true),
            CounterApplication.IDENTITY, CounterExample.LOG_ID, CounterExample.corpus()))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void warmedCounterTransitionAndSnapshotAllocateZeroWithWorkingAccounting() {
        final ThreadMXBean bean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        final CounterApplication application = new CounterApplication();
        final UnsafeBuffer input = hex("08000100f50101000100000000000000");
        final UnsafeBuffer result = new UnsafeBuffer(new byte[ApplicationLimits.DEFAULT.resultBytes()]);
        for (int i = 0; i < 60_000; i++) { cycle(application, input, result); }
        for (final int iterations : new int[]{20_000, 40_000}) {
            final long before = bean.getThreadAllocatedBytes(thread);
            for (int i = 0; i < iterations; i++) { cycle(application, input, result); }
            final long after = bean.getThreadAllocatedBytes(thread);
            assertThat(before).isNotNegative();
            assertThat(after).isGreaterThanOrEqualTo(before);
            assertThat(after - before).isZero();
        }
        final long before = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < 1000; i++) { escape = new byte[128]; }
        assertThat(bean.getThreadAllocatedBytes(thread) - before).isGreaterThanOrEqualTo(128_000);
    }

    private static void cycle(final CounterApplication application, final UnsafeBuffer input, final UnsafeBuffer result) {
        if (application.apply(input, 0, input.capacity(), null, 100, result, 0) != 25
            || application.snapshot(result, 0) != 16) { throw new AssertionError("counter fixture failed"); }
    }

    private static UnsafeBuffer hex(final String value) { return new UnsafeBuffer(HexFormat.of().parseHex(value)); }
    private static byte[] bytes(final DirectBuffer buffer, final int length) {
        final byte[] result = new byte[length];
        buffer.getBytes(0, result);
        return result;
    }

    private static final class BrokenApplication implements Application {
        private final CounterApplication delegate = new CounterApplication();
        private final int generation;
        private final boolean brokenRestore;
        BrokenApplication(final int generation, final boolean brokenRestore) {
            this.generation = generation;
            this.brokenRestore = brokenRestore;
        }
        public int apply(final DirectBuffer input, final int offset, final int length,
            final ObservationProposalDecoder proposal, final long time, final MutableDirectBuffer output, final int outputOffset) {
            final int size = delegate.apply(input, offset, length, proposal, time, output, outputOffset);
            output.putLong(outputOffset + 8, delegate.total() + generation, Wire.ORDER);
            return size;
        }
        public int snapshot(final MutableDirectBuffer output, final int offset) { return delegate.snapshot(output, offset); }
        public Application restore(final DirectBuffer input, final int offset, final int length, final long sequence) {
            return brokenRestore ? new CounterApplication() : delegate.restore(input, offset, length, sequence);
        }
        public void validateResult(final DirectBuffer input, final int offset, final int length) {
            delegate.validateResult(input, offset, length);
        }
    }

    private static final class BatchApplication implements Application {
        private final CounterApplication delegate;
        private final CounterBatchResultEncoder result = new CounterBatchResultEncoder();
        private final MessageHeaderEncoder header = new MessageHeaderEncoder();

        BatchApplication() { this(new CounterApplication()); }
        private BatchApplication(final CounterApplication delegate) { this.delegate = delegate; }

        public int apply(final DirectBuffer input, final int offset, final int length,
            final ObservationProposalDecoder proposal, final long time, final MutableDirectBuffer output, final int outputOffset) {
            delegate.apply(input, offset, length, proposal, time, output, outputOffset);
            final short accepted = output.getByte(outputOffset + 24);
            result.wrapAndApplyHeader(output, outputOffset, header).total(delegate.total()).loggedTimeNanos(time).accepted(accepted);
            final int count = (int)(delegate.total() % 32);
            final CounterBatchResultEncoder.EntriesEncoder entries = result.entriesCount(count);
            for (int i = 0; i < count; i++) { entries.next().index(i).value(delegate.total() + i); }
            return Wire.HEADER_LENGTH + result.encodedLength();
        }

        public int snapshot(final MutableDirectBuffer output, final int offset) { return delegate.snapshot(output, offset); }
        public Application restore(final DirectBuffer input, final int offset, final int length, final long sequence) {
            return new BatchApplication(delegate.restore(input, offset, length, sequence));
        }
        public void validateResult(final DirectBuffer input, final int offset, final int length) {
            if (!Wire.range(input, offset, length) || length < 29
                || input.getShort(offset, Wire.ORDER) != 17 || input.getShort(offset + 2, Wire.ORDER) != 4
                || input.getShort(offset + 4, Wire.ORDER) != 501 || input.getShort(offset + 6, Wire.ORDER) != 1
                || input.getShort(offset + 25, Wire.ORDER) != 12) { throw new IllegalArgumentException("invalid batch result"); }
            final CounterBatchResultDecoder decoded = new CounterBatchResultDecoder().wrap(input, offset + 8, 17, 1);
            final long total = decoded.total();
            final CounterBatchResultDecoder.EntriesDecoder entries = decoded.entries();
            if (total < 0 || total > delegate.total() || decoded.loggedTimeNanos() < 0 || decoded.accepted() > 1
                || entries.count() != total % 32 || length != 29 + entries.count() * 12) {
                throw new IllegalArgumentException("invalid batch result");
            }
            for (int i = 0; entries.hasNext(); i++) {
                entries.next();
                if (entries.index() != i || entries.value() != total + i) { throw new IllegalArgumentException("invalid batch entry"); }
            }
        }
    }
}
