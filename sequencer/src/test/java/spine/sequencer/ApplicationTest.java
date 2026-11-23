package spine.sequencer;

import com.sun.management.ThreadMXBean;
import spine.protocol.Wire;
import spine.protocol.ApplicationIdentity;
import spine.protocol.ApplicationLimits;
import spine.protocol.sbe.AdmissionStatus;
import spine.protocol.sbe.ObservationProposalDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A nonfinancial application exercises the public boundary without market code or schemas. */
class ApplicationTest {
    private static final long LOG = 11;
    private static final ApplicationIdentity IDENTITY = new ApplicationIdentity(501, 1, 1);
    private static volatile byte[] escapingAllocation;
    private static volatile long consumed;

    @Test
    void changingPrefixLengthsPreservesInputAndExactRetryWithoutRepeatingTheTransition() {
        final SequencerCore core = initialized(new CounterApplication());
        final int[] lengths = {0, 8, 16, ApplicationLimits.DEFAULT.resultBytes(), 8, 0};
        long total = 0;
        for (int i = 0; i < lengths.length; i++) {
            final int prefix = lengths[i];
            final UnsafeBuffer input = proposal(21, i + 1, i + 1, prefix, 20);
            assertThat(send(core, input, 100 + i)).isEqualTo(AdmissionStatus.ACCEPTED);
            total += i + 1;
            assertThat(application(core).total).isEqualTo(total);
            assertThat(application(core).calls).isEqualTo(i + 1);
            final byte[] original = event(core);
            final int payload = Wire.RESULT_OVERHEAD + Wire.EVENT_OVERHEAD;
            final int overhead = prefix == 0 ? 0 : prefix + 4;
            assertThat(core.eventLength()).isEqualTo(Wire.EVENT_OVERHEAD + 20 + overhead);
            if (prefix != 0) {
                assertThat(core.resultBuffer().getShort(payload, Wire.ORDER) & 65535).isEqualTo(prefix - 8);
                assertThat(core.resultBuffer().getInt(payload + prefix, Wire.ORDER)).isEqualTo(20);
                if (prefix >= 16) { assertThat(core.resultBuffer().getLong(payload + 8, Wire.ORDER)).isEqualTo(total); }
            }
            assertThat(bytes(core.resultBuffer(), payload + overhead, 20))
                .isEqualTo(bytes(input, Wire.PROPOSAL_OVERHEAD, 20));
            snapshot(core); // The next reply must use its cache after the shared output buffer was overwritten.
            assertThat(send(core, input, 999_999)).isEqualTo(AdmissionStatus.DUPLICATE);
            assertThat(event(core)).isEqualTo(original);
            assertThat(core.eventLength()).isZero();
            assertThat(application(core).calls).isEqualTo(i + 1);
        }
    }

    @Test
    void restoringAnEarlierSnapshotCannotLeakDiscardedProducerCacheIntoANewProducer() {
        for (final int prefixLength : new int[]{0, 8}) {
            final SequencerCore reused = initialized(new CounterApplication());
            final byte[] earlier = snapshot(reused);
            assertThat(send(reused, proposal(21, 1, 5, 92, 20), 100)).isEqualTo(AdmissionStatus.ACCEPTED);
            reused.restore(new UnsafeBuffer(earlier), 0, earlier.length);
            final SequencerCore fresh = new SequencerCore(IDENTITY, new CounterApplication());
            fresh.restore(new UnsafeBuffer(earlier), 0, earlier.length);
            final UnsafeBuffer next = proposal(22, 1, 7, prefixLength, 20);
            assertThat(send(reused, next, 200)).isEqualTo(AdmissionStatus.ACCEPTED);
            assertThat(send(fresh, next, 200)).isEqualTo(AdmissionStatus.ACCEPTED);
            assertThat(event(reused)).isEqualTo(event(fresh));
            assertThat(snapshot(reused)).as("new producer prefix length %s", prefixLength).isEqualTo(snapshot(fresh));
        }
    }

    @Test
    void otherProducerTransitionsAndSnapshotRestoreCannotChangeAnEarlierLatestReceipt() {
        final CounterApplication originalApplication = new CounterApplication();
        final SequencerCore core = initialized(originalApplication);
        final UnsafeBuffer first = proposal(21, 1, 5, ApplicationLimits.DEFAULT.resultBytes(), 20);
        assertThat(send(core, first, 100)).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] expected = event(core);
        assertThat(send(core, proposal(22, 1, 7, 16, 20), 200)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(send(core, first, 300)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(event(core)).isEqualTo(expected);
        assertThat(originalApplication.total).isEqualTo(12);
        assertThat(originalApplication.calls).isEqualTo(2);

        final byte[] saved = snapshot(core);
        final SequencerCore restored = new SequencerCore(IDENTITY, new CounterApplication());
        final UnsafeBuffer shifted = new UnsafeBuffer(new byte[saved.length + 13]);
        shifted.putBytes(13, saved);
        restored.restore(shifted, 13, saved.length);
        assertThat(application(restored)).isNotSameAs(originalApplication);
        assertThat(snapshot(restored)).isEqualTo(saved);
        assertThat(send(restored, first, 400)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(event(restored)).isEqualTo(expected);
        assertThat(application(restored).calls).isEqualTo(2);
        assertThat(application(restored).lastTime).isEqualTo(200);
        assertThat(restored.eventLength()).isZero();
    }

    @Test @Timeout(90)
    void injectedApplicationSurvivesClusterSnapshotSuffixReplayAndLostReceipts(@TempDir final Path directory) {
        final UnsafeBuffer first = proposal(21, 1, 5, 92, 20);
        final UnsafeBuffer suffix = proposal(22, 1, 7, 16, 20);
        final byte[] firstEvent, suffixEvent;
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(IDENTITY, new CounterApplication()), true)) {
            fixture.connect();
            exchange(fixture, initialize(), AdmissionStatus.INITIALIZED);
            firstEvent = exchange(fixture, first, AdmissionStatus.ACCEPTED);
            fixture.takeSnapshot();
            fixture.discardNextReply();
            suffixEvent = exchange(fixture, suffix, AdmissionStatus.DUPLICATE);
            fixture.abort();
        }
        final AtomicBoolean restored = new AtomicBoolean();
        final CounterApplication fresh = new CounterApplication() {
            @Override public Application restore(final DirectBuffer input, final int offset, final int length,
                final long sequence) {
                final Application candidate = super.restore(input, offset, length, sequence);
                restored.set(true);
                return candidate;
            }
        };
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(IDENTITY, fresh), false)) {
            fixture.connect();
            assertThat(restored).isTrue();
            assertThat(exchange(fixture, first, AdmissionStatus.DUPLICATE)).isEqualTo(firstEvent);
            assertThat(exchange(fixture, suffix, AdmissionStatus.DUPLICATE)).isEqualTo(suffixEvent);
            final byte[] next = exchange(fixture, proposal(21, 2, 11, 92, 20), AdmissionStatus.ACCEPTED);
            final UnsafeBuffer event = new UnsafeBuffer(next);
            assertThat(event.getLong(Wire.EVENT_OVERHEAD + 8, Wire.ORDER)).isEqualTo(23);
            assertThat(event.getLong(Wire.EVENT_OVERHEAD + 24, Wire.ORDER)).isEqualTo(3);
        }
    }

    @Test
    void everyInputCutRestoresApplicationStateAndAllSubsequentResultBytes() {
        final List<UnsafeBuffer> inputs = List.of(initialize(), proposal(21, 1, 2, 0, 20),
            proposal(22, 1, 3, 92, 20), proposal(21, 2, 5, 8, 20), proposal(22, 1, 3, 92, 20),
            proposal(22, 1, 7, 92, 20), proposal(21, 4, 7, 16, 20), proposal(21, 3, 7, 16, 20),
            proposal(21, 1, 2, 0, 20), proposal(22, 2, 11, 0, 20), proposal(21, 4, 13, 92, 20));
        final SequencerCore baseline = new SequencerCore(IDENTITY, new CounterApplication());
        final List<AdmissionStatus> statuses = new ArrayList<>();
        final List<byte[]> results = new ArrayList<>();
        final List<Integer> emitted = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            statuses.add(send(baseline, inputs.get(i), 100 + i));
            results.add(bytes(baseline.resultBuffer(), 0, baseline.resultLength()));
            emitted.add(baseline.eventLength());
        }
        assertThat(application(baseline).calls).isEqualTo(6);
        assertThat(application(baseline).total).isEqualTo(41);
        final byte[] expectedState = snapshot(baseline);
        for (int cut = 0; cut <= inputs.size(); cut++) {
            final SequencerCore prefix = new SequencerCore(IDENTITY, new CounterApplication());
            for (int i = 0; i < cut; i++) { send(prefix, inputs.get(i), 100 + i); }
            final byte[] saved = snapshot(prefix);
            final SequencerCore restored = new SequencerCore(IDENTITY, new CounterApplication());
            restored.restore(new UnsafeBuffer(saved), 0, saved.length);
            assertThat(snapshot(restored)).as("restored prefix at cut %s", cut).isEqualTo(saved);
            for (int i = cut; i < inputs.size(); i++) {
                assertThat(send(restored, inputs.get(i), 100 + i)).as("status at cut %s input %s", cut, i)
                    .isEqualTo(statuses.get(i));
                assertThat(bytes(restored.resultBuffer(), 0, restored.resultLength()))
                    .as("result at cut %s input %s", cut, i).isEqualTo(results.get(i));
                assertThat(restored.eventLength()).isEqualTo(emitted.get(i));
            }
            assertThat(snapshot(restored)).as("final state at cut %s", cut).isEqualTo(expectedState);
        }
    }

    @Test
    void failedCandidateOrCachedResultValidationLeavesTheWholeLiveStateUntouched() {
        final CounterApplication application = new CounterApplication();
        final SequencerCore core = initialized(application);
        final UnsafeBuffer input = proposal(21, 1, 5, 16, 20);
        assertThat(send(core, input, 100)).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] before = snapshot(core);
        final int stateOffset = Wire.SNAPSHOT_OVERHEAD + Wire.RECEIPT_OVERHEAD + 16 + 4;
        final int prefixOffset = Wire.SNAPSHOT_OVERHEAD + 68;
        final List<Consumer<UnsafeBuffer>> corruptions = List.of(
            b -> b.putLong(stateOffset, -1, Wire.ORDER),
            b -> b.putLong(stateOffset + 8, 2, Wire.ORDER),
            b -> b.putShort(prefixOffset + 4, (short)502, Wire.ORDER),
            b -> b.putLong(prefixOffset + 8, 6, Wire.ORDER),
            b -> b.putInt(Wire.SNAPSHOT_OVERHEAD + 64, 7, Wire.ORDER),
            b -> b.putInt(Wire.SNAPSHOT_OVERHEAD + 64, 93, Wire.ORDER),
            b -> b.putShort(prefixOffset, (short)7, Wire.ORDER));
        for (final Consumer<UnsafeBuffer> corrupt : corruptions) {
            final UnsafeBuffer candidate = new UnsafeBuffer(before.clone());
            corrupt.accept(candidate);
            SnapshotChecksum.seal(candidate);
            assertThatThrownBy(() -> core.restore(candidate, 0, candidate.capacity()))
                .isInstanceOf(IllegalArgumentException.class);
            assertThat(core.application()).isSameAs(application);
            assertThat(snapshot(core)).isEqualTo(before);
        }
        assertThat(send(core, input, 500)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(application.calls).isEqualTo(1);

        final SequencerCore passThrough = initialized(Application.PASS_THROUGH);
        final byte[] empty = snapshot(passThrough);
        assertThatThrownBy(() -> passThrough.restore(new UnsafeBuffer(before), 0, before.length))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(snapshot(passThrough)).isEqualTo(empty);
    }

    @Test
    void invalidApplicationOutputFailsInsteadOfReportingAnOrdinaryAdmissionRejection() {
        for (final int invalid : new int[]{-1, 7, 93, 16}) {
            final CounterApplication application = new CounterApplication() {
                @Override public int apply(final DirectBuffer input, final int offset, final int length,
                    final ObservationProposalDecoder proposal, final long time, final MutableDirectBuffer output,
                    final int destination) {
                    super.apply(input, offset, length, proposal, time, output, destination);
                    if (invalid == 16) { output.putShort(destination, (short)9, Wire.ORDER); }
                    return invalid;
                }
            };
            final SequencerCore core = initialized(application);
            final UnsafeBuffer input = proposal(21, 1, 5, 16, 20);
            assertThatThrownBy(() -> send(core, input, 100)).isInstanceOf(IllegalStateException.class);
            assertThat(application.calls).isEqualTo(1); // Mutation cannot be undone by an admission rejection.
            assertThat(core.sequence()).isZero();
            assertThat(core.eventLength()).isZero();
        }
    }

    @Test
    void neutralTransitionsRetriesAndSnapshotEncodingAllocateZeroAfterWarmup() {
        final SequencerCore core = initialized(new CounterApplication());
        final UnsafeBuffer input = proposal(21, 1, 1, 0, 20);
        final long[] sequence = {0};
        final int[] prefixes = {0, 8, 16, 92};
        final Runnable cycle = () -> {
            final long next = ++sequence[0];
            input.putLong(24, next, Wire.ORDER);
            input.putInt(Wire.PROPOSAL_OVERHEAD + 16, prefixes[(int)(next & 3)], Wire.ORDER);
            require(send(core, input, next) == AdmissionStatus.ACCEPTED);
            require(send(core, input, next + 100) == AdmissionStatus.DUPLICATE);
            consumed ^= core.snapshot();
        };
        proveNoAllocation(cycle, 100_000, 10_000, 20_000);
        assertThat(application(core).calls).isEqualTo(sequence[0]);
        assertThat(application(core).total).isEqualTo(sequence[0]);
    }

    @Test
    void maximumInputAndPrefixAdmissionAndRetryAllocateZeroAfterWarmup() {
        final SequencerCore core = initialized(new CounterApplication());
        final UnsafeBuffer input = proposal(21, 1, 1, 92, Wire.MAX_PAYLOAD_BYTES);
        final long[] sequence = {0};
        final Runnable cycle = () -> {
            input.putLong(24, ++sequence[0], Wire.ORDER);
            require(send(core, input, sequence[0]) == AdmissionStatus.ACCEPTED);
            require(core.eventLength() == core.limits().eventBytes());
            require(send(core, input, sequence[0] + 100) == AdmissionStatus.DUPLICATE);
            consumed ^= core.resultLength();
        };
        proveNoAllocation(cycle, 32_768, 2_048, 4_096);
        assertThat(application(core).calls).isEqualTo(sequence[0]);
    }

    static void proveNoAllocation(final Runnable action, final int warmup, final int small, final int large) {
        final java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assertThat(management).isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean)management;
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        if (!bean.isThreadAllocatedMemoryEnabled()) { bean.setThreadAllocatedMemoryEnabled(true); }
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        for (int i = 0; i < warmup; i++) { action.run(); }
        allocated(bean, thread, action, 32);
        final long first = allocated(bean, thread, action, small);
        final long second = allocated(bean, thread, action, large);
        assertThat(first).as("short allocation window").isZero();
        assertThat(second).as("long allocation window").isZero();
        assertThat(allocated(bean, thread, () -> escapingAllocation = new byte[64], 1024))
            .as("escaping allocations prove active accounting").isGreaterThanOrEqualTo(65_536);
        escapingAllocation = null;
    }

    private static long allocated(final ThreadMXBean bean, final long thread, final Runnable action, final int count) {
        final long before = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < count; i++) { action.run(); }
        final long after = bean.getThreadAllocatedBytes(thread);
        require(before >= 0 && after >= before);
        return after - before;
    }

    private static SequencerCore initialized(final Application application) {
        final SequencerCore core = new SequencerCore(IDENTITY, application);
        require(send(core, initialize(), 0) == AdmissionStatus.INITIALIZED);
        return core;
    }

    private static UnsafeBuffer initialize() {
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        new Wire().encodeInitialize(input, 0, LOG, IDENTITY);
        return input;
    }

    private static UnsafeBuffer proposal(final long producer, final long sequence, final long value,
        final int prefixLength, final int payloadLength) {
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[payloadLength]);
        payload.putShort(0, (short)12, Wire.ORDER);
        payload.putShort(2, (short)1, Wire.ORDER);
        payload.putShort(4, (short)501, Wire.ORDER);
        payload.putShort(6, (short)1, Wire.ORDER);
        payload.putLong(8, value, Wire.ORDER);
        payload.putInt(16, prefixLength, Wire.ORDER);
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payloadLength]);
        new Wire().encodeProposal(input, 0, LOG, producer, sequence, 1, 2, 3, 10, 11, 12,
            payload, 0, payloadLength);
        return input;
    }

    private static AdmissionStatus send(final SequencerCore core, final UnsafeBuffer input, final long time) {
        return core.onCommand(input, 0, input.capacity(), time, 1);
    }

    private static byte[] exchange(final ClusterFixture fixture, final UnsafeBuffer input, final AdmissionStatus status) {
        final byte[] reply = fixture.exchange(input, input.capacity());
        final UnsafeBuffer buffer = new UnsafeBuffer(reply);
        final Wire wire = new Wire();
        assertThat(wire.validResult(buffer, 0, reply.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(status);
        return bytes(buffer, Wire.RESULT_OVERHEAD, reply.length - Wire.RESULT_OVERHEAD);
    }

    private static CounterApplication application(final SequencerCore core) { return (CounterApplication)core.application(); }

    private static byte[] event(final SequencerCore core) {
        return bytes(core.resultBuffer(), Wire.RESULT_OVERHEAD, core.resultLength() - Wire.RESULT_OVERHEAD);
    }

    private static byte[] snapshot(final SequencerCore core) {
        final int length = core.snapshot();
        return bytes(core.resultBuffer(), 0, length);
    }

    private static byte[] bytes(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return bytes;
    }

    private static void require(final boolean condition) {
        if (!condition) { throw new AssertionError("application fixture invariant failed"); }
    }

    private static class CounterApplication implements Application {
        private long total, calls, lastTime;

        @Override public int apply(final DirectBuffer input, final int offset, final int length,
            final ObservationProposalDecoder proposal, final long timeNanos, final MutableDirectBuffer output,
            final int outputOffset) {
            require(proposal.sourceId() == 1 && proposal.sourceConfigVersion() == 2 && proposal.clockId() == 3);
            total += input.getLong(offset + 8, Wire.ORDER);
            calls++;
            lastTime = timeNanos;
            final int prefix = input.getInt(offset + 16, Wire.ORDER);
            if (prefix == 0) { return 0; }
            output.setMemory(outputOffset, prefix, (byte)0);
            output.putShort(outputOffset, (short)(prefix - 8), Wire.ORDER);
            output.putShort(outputOffset + 2, (short)2, Wire.ORDER);
            output.putShort(outputOffset + 4, (short)501, Wire.ORDER);
            output.putShort(outputOffset + 6, (short)1, Wire.ORDER);
            if (prefix >= 16) { output.putLong(outputOffset + 8, total, Wire.ORDER); }
            if (prefix >= 24) { output.putLong(outputOffset + 16, lastTime, Wire.ORDER); }
            if (prefix >= 32) { output.putLong(outputOffset + 24, calls, Wire.ORDER); }
            return prefix;
        }

        @Override public int snapshot(final MutableDirectBuffer output, final int offset) {
            output.putLong(offset, total, Wire.ORDER);
            output.putLong(offset + 8, calls, Wire.ORDER);
            output.putLong(offset + 16, lastTime, Wire.ORDER);
            return 24;
        }

        @Override public Application restore(final DirectBuffer input, final int offset, final int length,
            final long canonicalSequence) {
            if (length != 24) { throw new IllegalArgumentException("invalid counter state length"); }
            final CounterApplication candidate = new CounterApplication();
            candidate.total = input.getLong(offset, Wire.ORDER);
            candidate.calls = input.getLong(offset + 8, Wire.ORDER);
            candidate.lastTime = input.getLong(offset + 16, Wire.ORDER);
            if (candidate.total < 0 || candidate.calls != canonicalSequence || candidate.lastTime < 0
                || canonicalSequence == 0 && (candidate.total != 0 || candidate.lastTime != 0)) {
                throw new IllegalArgumentException("invalid counter state");
            }
            return candidate;
        }

        @Override public void validateResult(final DirectBuffer input, final int offset, final int length) {
            if (length == 0) { return; }
            if ((length != 8 && length != 16 && length != 92)
                || input.getShort(offset, Wire.ORDER) != length - 8
                || input.getShort(offset + 2, Wire.ORDER) != 2
                || input.getShort(offset + 4, Wire.ORDER) != 501 || input.getShort(offset + 6, Wire.ORDER) != 1
                || length >= 16 && (input.getLong(offset + 8, Wire.ORDER) < 0
                    || input.getLong(offset + 8, Wire.ORDER) > total)) {
                throw new IllegalArgumentException("invalid counter receipt");
            }
        }
    }
}
