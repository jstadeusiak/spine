package spine.testkit;

import spine.protocol.ApplicationIdentity;
import spine.protocol.Wire;
import spine.protocol.sbe.ObservationProposalDecoder;
import spine.sequencer.Application;
import spine.testkit.counter.sbe.*;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Neutral example: nonnegative additions, with deterministic rejection of malformed/overflowing additions. */
public final class CounterApplication implements Application {
    public static final ApplicationIdentity IDENTITY = new ApplicationIdentity(501, 1, 1);
    private final CounterCommandDecoder command = new CounterCommandDecoder();
    private final CounterResultEncoder result = new CounterResultEncoder();
    private final CounterSnapshotEncoder snapshot = new CounterSnapshotEncoder();
    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private long total;

    @Override public int apply(final DirectBuffer input, final int offset, final int length,
        final ObservationProposalDecoder proposal, final long timeNanos,
        final MutableDirectBuffer output, final int outputOffset) {
        boolean accepted = fixed(input, offset, length, CounterCommandDecoder.TEMPLATE_ID, CounterCommandDecoder.BLOCK_LENGTH);
        if (accepted) {
            command.wrap(input, offset + Wire.HEADER_LENGTH, CounterCommandDecoder.BLOCK_LENGTH, 1);
            final long delta = command.delta();
            accepted = delta >= 0 && delta <= Long.MAX_VALUE - total;
            if (accepted) { total += delta; }
        }
        result.wrapAndApplyHeader(output, outputOffset, header).total(total).loggedTimeNanos(timeNanos)
            .accepted((short)(accepted ? 1 : 0));
        return Wire.HEADER_LENGTH + CounterResultEncoder.BLOCK_LENGTH;
    }

    @Override public int snapshot(final MutableDirectBuffer output, final int offset) {
        snapshot.wrapAndApplyHeader(output, offset, header).total(total);
        return Wire.HEADER_LENGTH + CounterSnapshotEncoder.BLOCK_LENGTH;
    }

    @Override public CounterApplication restore(final DirectBuffer input, final int offset, final int length,
        final long sequence) {
        require(fixed(input, offset, length, CounterSnapshotDecoder.TEMPLATE_ID, CounterSnapshotDecoder.BLOCK_LENGTH));
        final long restored = new CounterSnapshotDecoder().wrap(input, offset + Wire.HEADER_LENGTH,
            CounterSnapshotDecoder.BLOCK_LENGTH, 1).total();
        require(restored >= 0 && (sequence != 0 || restored == 0));
        final CounterApplication candidate = new CounterApplication();
        candidate.total = restored;
        return candidate;
    }

    @Override public void validateResult(final DirectBuffer input, final int offset, final int length) {
        require(fixed(input, offset, length, CounterResultDecoder.TEMPLATE_ID, CounterResultDecoder.BLOCK_LENGTH));
        final CounterResultDecoder cached = new CounterResultDecoder().wrap(input, offset + Wire.HEADER_LENGTH,
            CounterResultDecoder.BLOCK_LENGTH, 1);
        require(cached.total() >= 0 && cached.total() <= total && cached.loggedTimeNanos() >= 0 && cached.accepted() <= 1);
    }

    public long total() { return total; }

    private static boolean fixed(final DirectBuffer input, final int offset, final int length,
        final int template, final int block) {
        return length == Wire.HEADER_LENGTH + block && Wire.range(input, offset, length)
            && input.getShort(offset, Wire.ORDER) == block && input.getShort(offset + 2, Wire.ORDER) == template
            && input.getShort(offset + 4, Wire.ORDER) == 501 && input.getShort(offset + 6, Wire.ORDER) == 1;
    }

    private static void require(final boolean valid) {
        if (!valid) { throw new IllegalArgumentException("invalid counter snapshot or cached result"); }
    }
}
