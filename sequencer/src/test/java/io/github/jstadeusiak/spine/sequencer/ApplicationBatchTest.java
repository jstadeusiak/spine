package io.github.jstadeusiak.spine.sequencer;

import io.github.jstadeusiak.spine.protocol.ApplicationIdentity;
import io.github.jstadeusiak.spine.protocol.ApplicationLimits;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import io.github.jstadeusiak.spine.protocol.sbe.ObservationProposalDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApplicationBatchTest {
    private static final ApplicationIdentity IDENTITY = new ApplicationIdentity(503, 1, 1);
    private static final ApplicationLimits LIMITS = new ApplicationLimits(2, Wire.MAX_APPLICATION_PREFIX_BYTES, 24);
    private static volatile long consumed;

    @Test void variableGroupsKeepExactOutputAcrossOtherProducersRestoreAndShorterReplacement() {
        final SequencerCore core = initialized(LIMITS);
        final UnsafeBuffer large = proposal(21, 1, 3, 1_100_000);
        assertThat(send(core, large, 10)).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] original = event(core);
        final int prefix = Wire.EVENT_OVERHEAD;
        final UnsafeBuffer encoded = new UnsafeBuffer(original);
        assertThat(encoded.getShort(prefix, Wire.ORDER)).isEqualTo((short)8);
        assertThat(encoded.getLong(prefix + 8, Wire.ORDER)).isEqualTo(3);
        assertThat(encoded.getShort(prefix + 18, Wire.ORDER)).isEqualTo((short)2);
        assertThat(encoded.getLong(prefix + 20, Wire.ORDER)).isEqualTo(1);
        assertThat(encoded.getInt(prefix + 28, Wire.ORDER)).isEqualTo(1_100_000 - 44);
        assertThat(encoded.getByte(prefix + 32)).isEqualTo((byte)3);
        assertThat(encoded.getLong(prefix + 1_100_000 - 12, Wire.ORDER)).isEqualTo(10);
        assertThat(encoded.getInt(prefix + 1_100_000, Wire.ORDER)).isEqualTo(20);
        assertThat(bytes(encoded, prefix + 1_100_000 + 4, 20))
            .isEqualTo(bytes(large, Wire.PROPOSAL_OVERHEAD, 20));

        assertThat(send(core, proposal(22, 1, 5, 44), 20)).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] saved = snapshot(core);
        assertThat(saved.length).isEqualTo(Wire.SNAPSHOT_OVERHEAD + 2 * Wire.RECEIPT_OVERHEAD + 1_100_000 + 44 + 4 + 24);
        final SequencerCore restored = initialized(LIMITS);
        final UnsafeBuffer shifted = new UnsafeBuffer(new byte[saved.length + 9]);
        shifted.putBytes(9, saved);
        restored.restore(shifted, 9, saved.length);
        assertThat(snapshot(restored)).isEqualTo(saved);
        assertThat(send(restored, large, 999)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(event(restored)).isEqualTo(original);
        assertThat(restored.eventLength()).isZero();
        assertThat(((Batch)restored.application()).calls).isEqualTo(2);

        final UnsafeBuffer shorter = proposal(21, 2, 7, 44);
        assertThat(send(restored, shorter, 30)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(snapshot(restored).length).isEqualTo(Wire.SNAPSHOT_OVERHEAD + 2 * (Wire.RECEIPT_OVERHEAD + 44) + 4 + 24);
        final byte[] shorterEvent = eventAfterRetry(restored, shorter);
        final SequencerCore fresh = initialized(LIMITS);
        send(fresh, large, 10);
        send(fresh, proposal(22, 1, 5, 44), 20);
        send(fresh, shorter, 30);
        assertThat(event(fresh)).isEqualTo(shorterEvent);
        assertThat(snapshot(restored)).isEqualTo(snapshot(fresh));
    }

    @Test void localBoundsRejectRestoreWithoutReplacingAnyLiveStateOrLatestReceipt() {
        final SequencerCore source = initialized(LIMITS);
        send(source, proposal(21, 1, 3, 1024), 10);
        send(source, proposal(22, 1, 5, 44), 20);
        final byte[] saved = snapshot(source);
        for (final ApplicationLimits limit : new ApplicationLimits[]{
            new ApplicationLimits(1, 1024, 24), new ApplicationLimits(2, 1023, 24),
            new ApplicationLimits(2, 1024, 23)
        }) {
            final SequencerCore target = initialized(limit);
            final Application owner = target.application();
            assertThatThrownBy(() -> target.restore(new UnsafeBuffer(saved), 0, saved.length))
                .isInstanceOf(IllegalArgumentException.class);
            assertThat(target.sequence()).isZero();
            assertThat(target.application()).isSameAs(owner);
            assertThat(((Batch)owner).total).isZero();
            assertThat(((Batch)owner).calls).isZero();
            assertThat(((Batch)owner).time).isZero();
        }
        final UnsafeBuffer invalid = new UnsafeBuffer(saved.clone());
        invalid.putInt(Wire.SNAPSHOT_OVERHEAD + Wire.RECEIPT_OVERHEAD + 28, 1024, Wire.ORDER);
        SnapshotChecksum.seal(invalid);
        assertThatThrownBy(() -> source.restore(invalid, 0, invalid.capacity())).isInstanceOf(IllegalArgumentException.class);
        assertThat(snapshot(source)).isEqualTo(saved);
        assertThat(send(source, proposal(21, 1, 3, 1024), 500)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(((Batch)source.application()).calls).isEqualTo(2);
    }

    @Test void declaredProducerCapacityAndTinySnapshotStorageStillAllowMaximumInputReceipts() {
        final ApplicationLimits limits = new ApplicationLimits(1, 8, 0);
        final SequencerCore core = new SequencerCore(IDENTITY, Application.PASS_THROUGH, limits);
        initialize(core);
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.MAX_PROPOSAL_BYTES]);
        final int length = new Wire().encodeProposal(input, 0, 11, 21, 1, 1, 1, 1, -1, -1, -1,
            payload, 0, payload.capacity());
        assertThat(core.onCommand(input, 0, length, 10, 1)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(core.resultLength()).isEqualTo(Wire.RESULT_OVERHEAD + Wire.EVENT_OVERHEAD + payload.capacity());
        assertThat(send(core, proposal(22, 1, 1, 0), 20)).isEqualTo(AdmissionStatus.CAPACITY);
        assertThat(core.sequence()).isEqualTo(1);
        assertThat(core.snapshot()).isEqualTo(Wire.SNAPSHOT_OVERHEAD + Wire.RECEIPT_OVERHEAD + 4);
    }

    @Test void maximumVariableResultAdmissionSnapshotAndRetryAllocateZeroAfterWarmup() {
        final SequencerCore core = initialized(new ApplicationLimits(1, Wire.MAX_APPLICATION_PREFIX_BYTES, 24));
        final UnsafeBuffer input = proposal(21, 1, 1, 44);
        final long[] sequence = {0};
        final Runnable cycle = () -> {
            input.putLong(24, ++sequence[0], Wire.ORDER);
            if (send(core, input, sequence[0]) != AdmissionStatus.ACCEPTED) { throw new AssertionError(); }
            consumed ^= core.snapshot();
            if (send(core, input, sequence[0] + 1) != AdmissionStatus.DUPLICATE) { throw new AssertionError(); }
            consumed ^= core.resultLength();
        };
        for (int i = 0; i < 32_768; i++) { cycle.run(); }
        input.putInt(Wire.PROPOSAL_OVERHEAD + 16, Wire.MAX_APPLICATION_PREFIX_BYTES, Wire.ORDER);
        ApplicationTest.proveNoAllocation(cycle, 1_024, 128, 256);
        assertThat(core.resultLength()).isEqualTo(Wire.RESULT_OVERHEAD + Wire.EVENT_OVERHEAD
            + Wire.MAX_APPLICATION_PREFIX_BYTES + 4 + 20);
    }

    private static SequencerCore initialized(final ApplicationLimits limits) {
        final SequencerCore core = new SequencerCore(IDENTITY, new Batch(), limits);
        initialize(core);
        return core;
    }

    private static void initialize(final SequencerCore core) {
        final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        new Wire().encodeInitialize(command, 0, 11, IDENTITY);
        assertThat(send(core, command, 0)).isEqualTo(AdmissionStatus.INITIALIZED);
    }

    private static UnsafeBuffer proposal(final long producer, final long sequence, final long delta, final int resultLength) {
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[20]);
        payload.putLong(8, delta, Wire.ORDER);
        payload.putInt(16, resultLength, Wire.ORDER);
        final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payload.capacity()]);
        new Wire().encodeProposal(command, 0, 11, producer, sequence, 1, 1, 1, -1, -1, -1,
            payload, 0, payload.capacity());
        return command;
    }

    private static AdmissionStatus send(final SequencerCore core, final UnsafeBuffer input, final long time) {
        return core.onCommand(input, 0, input.capacity(), time, 1);
    }

    private static byte[] eventAfterRetry(final SequencerCore core, final UnsafeBuffer input) {
        assertThat(send(core, input, 999)).isEqualTo(AdmissionStatus.DUPLICATE);
        return event(core);
    }

    private static byte[] event(final SequencerCore core) {
        return bytes(core.resultBuffer(), Wire.RESULT_OVERHEAD, core.resultLength() - Wire.RESULT_OVERHEAD);
    }

    private static byte[] snapshot(final SequencerCore core) {
        return bytes(core.resultBuffer(), 0, core.snapshot());
    }

    private static byte[] bytes(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return bytes;
    }

    /** Literal SBE fixture: total, two entries with fixed sequence/time and variable data, then Spine's input. */
    private static final class Batch implements Application {
        private long total, calls, time;

        public int apply(final DirectBuffer input, final int offset, final int length,
            final ObservationProposalDecoder proposal, final long timeNanos, final MutableDirectBuffer output,
            final int outputOffset) {
            total += input.getLong(offset + 8, Wire.ORDER);
            calls++;
            time = timeNanos;
            final int size = input.getInt(offset + 16, Wire.ORDER);
            if (size == 0) { return 0; }
            if (size < 44) { throw new IllegalArgumentException("invalid batch size"); }
            output.setMemory(outputOffset, size, (byte)total);
            output.putShort(outputOffset, (short)8, Wire.ORDER);
            output.putShort(outputOffset + 2, (short)2, Wire.ORDER);
            output.putShort(outputOffset + 4, (short)503, Wire.ORDER);
            output.putShort(outputOffset + 6, (short)1, Wire.ORDER);
            output.putLong(outputOffset + 8, total, Wire.ORDER);
            output.putShort(outputOffset + 16, (short)8, Wire.ORDER);
            output.putShort(outputOffset + 18, (short)2, Wire.ORDER);
            output.putLong(outputOffset + 20, calls, Wire.ORDER);
            output.putInt(outputOffset + 28, size - 44, Wire.ORDER);
            output.putLong(outputOffset + size - 12, time, Wire.ORDER);
            output.putInt(outputOffset + size - 4, 0, Wire.ORDER);
            return size;
        }

        public int snapshot(final MutableDirectBuffer output, final int offset) {
            output.putLong(offset, total, Wire.ORDER);
            output.putLong(offset + 8, calls, Wire.ORDER);
            output.putLong(offset + 16, time, Wire.ORDER);
            return 24;
        }

        public Application restore(final DirectBuffer input, final int offset, final int length, final long sequence) {
            if (length != 24) { throw new IllegalArgumentException("invalid batch state"); }
            final Batch candidate = new Batch();
            candidate.total = input.getLong(offset, Wire.ORDER);
            candidate.calls = input.getLong(offset + 8, Wire.ORDER);
            candidate.time = input.getLong(offset + 16, Wire.ORDER);
            if (candidate.total < 0 || candidate.calls != sequence || candidate.time < 0) {
                throw new IllegalArgumentException("invalid batch state");
            }
            return candidate;
        }

        public void validateResult(final DirectBuffer input, final int offset, final int length) {
            if (length == 0) { return; }
            if (length < 44 || input.getShort(offset, Wire.ORDER) != 8
                || input.getShort(offset + 2, Wire.ORDER) != 2 || input.getShort(offset + 4, Wire.ORDER) != 503
                || input.getShort(offset + 6, Wire.ORDER) != 1 || input.getShort(offset + 16, Wire.ORDER) != 8
                || input.getShort(offset + 18, Wire.ORDER) != 2 || input.getInt(offset + 28, Wire.ORDER) != length - 44
                || input.getInt(offset + length - 4, Wire.ORDER) != 0
                || input.getLong(offset + 8, Wire.ORDER) < 0 || input.getLong(offset + 8, Wire.ORDER) > total
                || input.getLong(offset + 20, Wire.ORDER) < 1 || input.getLong(offset + 20, Wire.ORDER) > calls
                || input.getLong(offset + length - 12, Wire.ORDER) < 0 || input.getLong(offset + length - 12, Wire.ORDER) > time) {
                throw new IllegalArgumentException("invalid cached batch");
            }
        }
    }
}
