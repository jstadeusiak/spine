package spine.protocol;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.assertj.core.api.Assertions.*;

class CaptureCodecTest {
    private final CaptureCodec codec = new CaptureCodec();
    private final UnsafeBuffer dst = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES + 16]);
    private final UnsafeBuffer raw = new UnsafeBuffer(new byte[]{0, 1, 2, -1, 4});

    @Test void numericNativeIdentityAndFrameSequenceRemainPrimitiveAndUnsigned() {
        final int length = codec.encodeObservation(dst, 7, Long.MAX_VALUE, Integer.MIN_VALUE, -2, null, 0, 0, raw, 0, 5);
        assertThat(length).isEqualTo(41);
        assertThat(codec.validObservation(dst, 7, length)).isTrue();
        assertThat(codec.observation.rawFrameSequence()).isEqualTo(Long.MAX_VALUE);
        assertThat(codec.observation.subeventIndex()).isEqualTo(2_147_483_648L);
        assertThat(codec.observation.nativeNumericId()).isEqualTo(-2);
        assertThat(codec.nativeLength).isZero();
        assertThat(codec.buffer).isSameAs(dst);
        assertThat(codec.rawOffset).isEqualTo(7 + CaptureCodec.OBSERVATION_OVERHEAD);
        assertThat(codec.rawLength).isEqualTo(5);
        assertThat(codec.buffer.getByte(codec.rawOffset + 3)).isEqualTo((byte) -1);
    }

    @Test void nonnumericIdentityPreservesExactBytesAndLeadingZerosWithoutUtf8Conversion() {
        final UnsafeBuffer nativeId = new UnsafeBuffer(new byte[]{'0', '0', '1', 0, -1});
        final int length = codec.encodeObservation(dst, 0, 9, 0, CaptureCodec.UNKNOWN_NATIVE_ID,
            nativeId, 0, 5, raw, 0, 5);
        assertThat(codec.validObservation(dst, 0, length)).isTrue();
        assertThat(codec.nativeLength).isEqualTo(5);
        final byte[] read = new byte[5];
        codec.buffer.getBytes(codec.nativeOffset, read);
        assertThat(read).containsExactly(nativeId.byteArray());
        dst.putByte(codec.nativeOffset, (byte) '7');
        assertThat(codec.buffer.getByte(codec.nativeOffset)).isEqualTo((byte) '7');
        assertThatThrownBy(() -> codec.encodeObservation(dst, 0, 9, 0, 1, nativeId, 0, 5, raw, 0, 5))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void maximumCompleteFrameFitsAndOversizedCaptureFailsBeforeChangingDestination() {
        final UnsafeBuffer large = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES - CaptureCodec.OBSERVATION_OVERHEAD]);
        large.putByte(large.capacity() - 1, (byte) 77);
        final int length = codec.encodeObservation(dst, 0, 1, 0, -1, null, 0, 0, large, 0, large.capacity());
        assertThat(length).isEqualTo(Wire.MAX_PAYLOAD_BYTES);
        assertThat(codec.validObservation(dst, 0, length)).isTrue();
        assertThat(codec.buffer.getByte(codec.rawOffset + codec.rawLength - 1)).isEqualTo((byte) 77);
        final byte[] before = dst.byteArray().clone();
        assertThatThrownBy(() -> codec.encodeObservation(dst, 0, 1, 0, -1, null, 0, 0,
            dst, 0, large.capacity() + 1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(dst.byteArray()).containsExactly(before);
    }

    @Test void rejectsReservedFieldsTruncationAndAmbiguousNativeIdentity() {
        final int length = codec.encodeObservation(dst, 0, 1, 0, -1, null, 0, 0, raw, 0, 5);
        assertThat(codec.validObservation(dst, 0, length - 1)).isFalse();
        assertThat(codec.validObservation(dst, 0, length + 1)).isFalse();
        dst.putLong(8, -1, Wire.ORDER);
        assertThat(codec.validObservation(dst, 0, length)).isFalse();
        dst.putLong(8, 1, Wire.ORDER);
        dst.putInt(16, -1, Wire.ORDER);
        assertThat(codec.validObservation(dst, 0, length)).isFalse();
        dst.putInt(16, 0, Wire.ORDER);
        dst.putInt(28, Integer.MAX_VALUE, Wire.ORDER);
        assertThat(codec.validObservation(dst, 0, length)).isFalse();
        dst.putInt(28, -1, Wire.ORDER);
        assertThat(codec.validObservation(dst, 0, length)).isFalse();
        final UnsafeBuffer id = new UnsafeBuffer(new byte[]{1});
        final int bytesLength = codec.encodeObservation(dst, 0, 1, 0, -1, id, 0, 1, raw, 0, 5);
        dst.putLong(20, 9, Wire.ORDER);
        assertThat(codec.validObservation(dst, 0, bytesLength)).isFalse();
    }

    @Test void coldDefinitionsRetainSixUtf8LabelsAndDeclaredUncertainty() {
        final byte[][] labels = labels("provider", "venue:BTC", "session-1", "collector", "config", "UTC±é");
        final int length = codec.encodeDefinition(dst, 4, -1, labels);
        assertThat(codec.validDefinition(dst, 4, length)).isTrue();
        assertThat(codec.definition.syncUncertaintyNanos()).isEqualTo(-1);
        for (int i = 0; i < labels.length; i++) {
            final byte[] bytes = new byte[codec.labelLength(i)];
            codec.buffer.getBytes(codec.labelOffset(i), bytes);
            assertThat(bytes).containsExactly(labels[i]);
        }
        assertThat(codec.validDefinition(dst, 4, length - 1)).isFalse();
        assertThat(codec.validDefinition(dst, 4, length + 1)).isFalse();
        dst.putLong(12, -2, Wire.ORDER);
        assertThat(codec.validDefinition(dst, 4, length)).isFalse();
    }

    @Test void coldDefinitionsRejectMalformedUtf8BlankOrMissingLabels() {
        for (final byte[] invalid : new byte[][]{new byte[0], new byte[]{' '}, new byte[]{(byte) 0xC3}, new byte[4097]}) {
            final byte[][] labels = labels("p", "p", "s", "c", "c", "t");
            labels[0] = invalid;
            assertThatThrownBy(() -> codec.encodeDefinition(dst, 0, 0, labels)).isInstanceOf(IllegalArgumentException.class);
        }
        final int length = codec.encodeDefinition(dst, 0, 0, labels("p", "p", "s", "c", "c", "t"));
        dst.putByte(20, (byte) 0xFF);
        assertThat(codec.validDefinition(dst, 0, length)).isFalse();
    }

    @Test void malformedOrdinaryCaptureAndRangesNeverThrow() {
        final byte[] bytes = new byte[128];
        final UnsafeBuffer input = new UnsafeBuffer(bytes);
        final Random random = new Random(12);
        for (int i = 0; i < 10000; i++) {
            random.nextBytes(bytes);
            codec.validObservation(input, random.nextInt(132) - 2, random.nextInt(132) - 2);
        }
        assertThat(codec.validObservation(null, 0, 8)).isFalse();
        assertThat(codec.validObservation(input, Integer.MAX_VALUE, Integer.MAX_VALUE)).isFalse();
    }

    private static byte[][] labels(final String... values) {
        final byte[][] labels = new byte[values.length][];
        for (int i = 0; i < values.length; i++) labels[i] = values[i].getBytes(StandardCharsets.UTF_8);
        return labels;
    }
}
