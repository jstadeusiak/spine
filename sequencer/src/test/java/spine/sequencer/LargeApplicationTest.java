package spine.sequencer;

import spine.protocol.ApplicationIdentity;
import spine.protocol.ApplicationLimits;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import spine.protocol.sbe.ObservationProposalDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LargeApplicationTest {
    private static final ApplicationIdentity IDENTITY = new ApplicationIdentity(602, 1, 1);
    private static final int ROWS = 65_536;
    private static final int RESULT_BYTES = 20 + ROWS * 16;
    private static final ApplicationLimits LIMITS = new ApplicationLimits(2, RESULT_BYTES, 16);
    private static final String LARGE_OUTPUT = "aeron:ipc?term-length=32m|alias=spine-output";

    @Test @Timeout(90)
    void nativeLargeBatchLostReceiptAndSnapshotRetryKeepEveryOutputByte(@TempDir final Path directory) {
        final UnsafeBuffer first = proposal(1, 1, ROWS);
        final byte[] firstEvent;
        final byte[] secondEvent;
        try (ClusterFixture fixture = new ClusterFixture(directory, service(), true)) {
            fixture.connect("aeron:ipc?term-length=32m");
            exchange(fixture, initialize(), AdmissionStatus.INITIALIZED);
            fixture.discardNextReply();
            firstEvent = exchange(fixture, first, AdmissionStatus.DUPLICATE);
            assertThat(firstEvent.length).isEqualTo(Wire.EVENT_OVERHEAD + RESULT_BYTES + 4 + 16);
            secondEvent = exchange(fixture, proposal(2, 1, 3), AdmissionStatus.ACCEPTED);
            assertThat(exchange(fixture, first, AdmissionStatus.DUPLICATE)).isEqualTo(firstEvent);
            fixture.takeSnapshot();
            fixture.abort();
        }
        try (ClusterFixture fixture = new ClusterFixture(directory, service(), false)) {
            fixture.connect("aeron:ipc?term-length=32m");
            assertThat(exchange(fixture, first, AdmissionStatus.DUPLICATE)).isEqualTo(firstEvent);
            assertThat(exchange(fixture, proposal(2, 1, 3), AdmissionStatus.DUPLICATE)).isEqualTo(secondEvent);
            exchange(fixture, proposal(3, 1, 1), AdmissionStatus.CAPACITY);
            final byte[] shorter = exchange(fixture, proposal(1, 2, 0), AdmissionStatus.ACCEPTED);
            assertThat(shorter.length).isEqualTo(Wire.EVENT_OVERHEAD + 20 + 4 + 16);
            assertThat(exchange(fixture, proposal(1, 2, 0), AdmissionStatus.DUPLICATE)).isEqualTo(shorter);
        }
    }

    @Test @Timeout(45)
    void outputTransportMustFitDeclaredResultsBeforeReadiness(@TempDir final Path directory) {
        assertThatThrownBy(() -> {
            try (ClusterFixture fixture = new ClusterFixture(directory,
                new SequencerService(IDENTITY, new Batch(), LIMITS,
                    SequencerService.OUTPUT_CHANNEL, SequencerService.OUTPUT_STREAM), true)) {
                fixture.connect();
                fixture.await(() -> false, () -> { }, "output capacity rejection");
            }
        }).isInstanceOf(RuntimeException.class).hasStackTraceContaining("too small for declared");
    }

    private static SequencerService service() {
        return new SequencerService(IDENTITY, new Batch(), LIMITS, LARGE_OUTPUT, SequencerService.OUTPUT_STREAM);
    }

    private static UnsafeBuffer initialize() {
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        new Wire().encodeInitialize(input, 0, 42, IDENTITY);
        return input;
    }

    private static UnsafeBuffer proposal(final long producer, final long sequence, final int rows) {
        final UnsafeBuffer payload = SequencerCoreTest.payload(rows, 16);
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payload.capacity()]);
        new Wire().encodeProposal(input, 0, 42, producer, sequence, 1, 1, 1, -1, -1, -1,
            payload, 0, payload.capacity());
        return input;
    }

    private static byte[] exchange(final ClusterFixture fixture, final UnsafeBuffer input,
        final AdmissionStatus expected) {
        final byte[] reply = fixture.exchange(input, input.capacity());
        final Wire wire = new Wire();
        assertThat(wire.validResult(new UnsafeBuffer(reply), 0, reply.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(expected);
        return Arrays.copyOfRange(reply, Wire.RESULT_OVERHEAD, reply.length);
    }

    private static final class Batch implements Application {
        private long total;
        private long calls;

        @Override public int apply(final DirectBuffer input, final int offset, final int length,
            final ObservationProposalDecoder proposal, final long timeNanos,
            final MutableDirectBuffer output, final int outputOffset) {
            final int rows = (int)input.getLong(offset + 8, Wire.ORDER);
            if (rows < 0 || rows > ROWS) { throw new IllegalArgumentException("invalid row count"); }
            total += rows;
            calls++;
            header(output, outputOffset, 1);
            output.putLong(outputOffset + 8, total, Wire.ORDER);
            output.putInt(outputOffset + 16, rows * 16, Wire.ORDER);
            for (int i = 0; i < rows; i++) {
                final int row = outputOffset + 20 + i * 16;
                header(output, row, 2);
                output.putLong(row + 8, total + i, Wire.ORDER);
            }
            return 20 + rows * 16;
        }

        private static void header(final MutableDirectBuffer output, final int offset, final int template) {
            output.putShort(offset, (short)8, Wire.ORDER);
            output.putShort(offset + 2, (short)template, Wire.ORDER);
            output.putShort(offset + 4, (short)602, Wire.ORDER);
            output.putShort(offset + 6, (short)1, Wire.ORDER);
        }

        @Override public int snapshot(final MutableDirectBuffer output, final int offset) {
            output.putLong(offset, total, Wire.ORDER);
            output.putLong(offset + 8, calls, Wire.ORDER);
            return 16;
        }

        @Override public Application restore(final DirectBuffer input, final int offset, final int length,
            final long sequence) {
            if (length != 16 || input.getLong(offset, Wire.ORDER) < 0
                || input.getLong(offset + 8, Wire.ORDER) != sequence) {
                throw new IllegalArgumentException("invalid batch state");
            }
            final Batch restored = new Batch();
            restored.total = input.getLong(offset, Wire.ORDER);
            restored.calls = sequence;
            return restored;
        }

        @Override public void validateResult(final DirectBuffer input, final int offset, final int length) {
            if (length < 20 || (length - 20) % 16 != 0
                || input.getShort(offset, Wire.ORDER) != 8 || input.getShort(offset + 2, Wire.ORDER) != 1
                || input.getShort(offset + 4, Wire.ORDER) != 602 || input.getShort(offset + 6, Wire.ORDER) != 1
                || input.getInt(offset + 16, Wire.ORDER) != length - 20) {
                throw new IllegalArgumentException("invalid batch result");
            }
            final long sum = input.getLong(offset + 8, Wire.ORDER);
            if (sum < 0 || sum > total) { throw new IllegalArgumentException("invalid batch total"); }
            for (int i = 0; i < (length - 20) / 16; i++) {
                final int row = offset + 20 + i * 16;
                if (input.getShort(row, Wire.ORDER) != 8 || input.getShort(row + 2, Wire.ORDER) != 2
                    || input.getShort(row + 4, Wire.ORDER) != 602 || input.getShort(row + 6, Wire.ORDER) != 1
                    || input.getLong(row + 8, Wire.ORDER) != sum + i) {
                    throw new IllegalArgumentException("invalid nested result");
                }
            }
        }
    }
}
