package io.github.jstadeusiak.spine.protocol;

/** Cold, preallocated bounds; a different declaration requires a different application configuration identity. */
public record ApplicationLimits(int producers, int resultBytes, int stateBytes) {
    public static final ApplicationLimits DEFAULT = new ApplicationLimits(1024, 92, 2 * 1024 * 1024);

    public ApplicationLimits {
        if (producers < 1 || producers > Wire.MAX_PRODUCERS
            || resultBytes < Wire.HEADER_LENGTH || resultBytes > Wire.MAX_APPLICATION_PREFIX_BYTES
            || stateBytes < 0 || stateBytes > Wire.MAX_APPLICATION_STATE_BYTES
            || Wire.SNAPSHOT_OVERHEAD + (long)producers * (Wire.RECEIPT_OVERHEAD + resultBytes) + 4 + stateBytes
                > Wire.MAX_SNAPSHOT_BYTES) {
            throw new IllegalArgumentException("invalid application capacity declaration");
        }
    }

    public int snapshotBytes() {
        return Wire.SNAPSHOT_OVERHEAD + producers * (Wire.RECEIPT_OVERHEAD + resultBytes) + 4 + stateBytes;
    }

    public int eventBytes() { return Wire.EVENT_OVERHEAD + resultBytes + 4 + Wire.MAX_PAYLOAD_BYTES; }
}
