package io.github.jstadeusiak.spine.testkit;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

/** Synthetic SBE-shaped payload: an 8-byte fixed value, then length-prefixed padding. */
public final class ExamplePayload {
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    private ExamplePayload() { }

    public static UnsafeBuffer encode(final long value, final int length) {
        if (length < 20 || length > 65536) {
            throw new IllegalArgumentException("example payload length must be 20..65536");
        }
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        buffer.putShort(0, (short)8, ORDER);
        buffer.putShort(2, (short)1, ORDER);
        buffer.putShort(4, (short)301, ORDER);
        buffer.putShort(6, (short)1, ORDER);
        buffer.putLong(8, value, ORDER);
        buffer.putInt(16, length - 20, ORDER);
        for (int i = 20; i < length; i++) {
            buffer.putByte(i, (byte)(value + i));
        }
        return buffer;
    }

    public static long value(final DirectBuffer buffer, final int offset, final int length) {
        if (length < 20 || buffer.getShort(offset, ORDER) != 8 || buffer.getShort(offset + 2, ORDER) != 1
            || buffer.getShort(offset + 4, ORDER) != 301 || buffer.getShort(offset + 6, ORDER) != 1
            || buffer.getInt(offset + 16, ORDER) != length - 20) {
            throw new IllegalArgumentException("invalid example payload");
        }
        return buffer.getLong(offset + 8, ORDER);
    }
}
