package io.github.jstadeusiak.spine.testkit;

import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.SequencedEventDecoder;
import org.agrona.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Small deterministic state machine used to compare full and checkpointed replay. */
public final class CountingProjection {
    public static final String INTERPRETER = "counting-projection-v1";
    public static final int STATE_VERSION = 1;
    private long count;
    private long sum;
    private long digest = 0xcbf29ce484222325L;

    public void apply(final DirectBuffer buffer, final int offset, final int length, final SequencedEventDecoder event) {
        final int payloadOffset = offset + Wire.EVENT_OVERHEAD;
        final int payloadLength = length - Wire.EVENT_OVERHEAD;
        final long value = ExamplePayload.value(buffer, payloadOffset, payloadLength);
        if (value != count + 1) {
            throw new IllegalStateException("example state expected value " + (count + 1) + ", received " + value);
        }
        for (int i = 0; i < payloadLength; i++) {
            digest ^= buffer.getByte(payloadOffset + i) & 0xff;
            digest *= 0x100000001b3L;
        }
        count++;
        sum += value;
    }

    public long count() { return count; }
    public long sum() { return sum; }
    public long digest() { return digest; }

    public byte[] snapshot() {
        return ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
            .putLong(count).putLong(sum).putLong(digest).array();
    }

    public void restore(final byte[] state) {
        if (state.length != 24) { throw new IllegalArgumentException("invalid projection state length"); }
        final ByteBuffer buffer = ByteBuffer.wrap(state).order(ByteOrder.LITTLE_ENDIAN);
        count = buffer.getLong();
        sum = buffer.getLong();
        digest = buffer.getLong();
        if (count < 0 || sum != count * (count + 1) / 2) {
            throw new IllegalArgumentException("invalid projection state");
        }
    }
}
