package spine.sequencer;

import spine.protocol.sbe.ObservationProposalDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * One deterministic owner; all callback buffers/codecs are borrowed. Preallocate before admission; no clocks,
 * external effects or steady-state allocation. Compose application components directly in a fixed order.
 */
public interface Application {
    /**
     * Called once per admitted input, never for a retry. Return zero for the original input alone, or write an
     * SBE header, fixed block and optional groups/data within the declared result bound. Spine appends a uint32
     * length and the exact original input. Output has only the declared capacity. Contract failure after mutation is fatal.
     */
    int apply(DirectBuffer input, int offset, int length, ObservationProposalDecoder proposal, long timeNanos,
              MutableDirectBuffer output, int outputOffset);

    /** Canonical complete state within the declared application-state bound; no allocation. */
    int snapshot(MutableDirectBuffer output, int offset);

    /** Cold: return a fully validated private candidate, leaving this owner untouched on success or failure. */
    Application restore(DirectBuffer input, int offset, int length, long sequence);

    /** Cold: validate a cached result prefix (zero denotes passthrough) without changing the candidate. */
    void validateResult(DirectBuffer input, int offset, int length);

    /** Immutable observation-only application. No application state or result prefix. */
    Application PASS_THROUGH = new Application() {
        public int apply(final DirectBuffer input, final int offset, final int length,
            final ObservationProposalDecoder proposal, final long timeNanos,
            final MutableDirectBuffer output, final int outputOffset) { return 0; }
        public int snapshot(final MutableDirectBuffer output, final int offset) { return 0; }
        public Application restore(final DirectBuffer input, final int offset, final int length, final long sequence) {
            validateResult(input, offset, length);
            return this;
        }
        public void validateResult(final DirectBuffer input, final int offset, final int length) {
            if (length != 0) { throw new IllegalArgumentException("nonempty passthrough application state or result"); }
        }
    };
}
