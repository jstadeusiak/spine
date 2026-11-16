package io.github.jstadeusiak.spine.protocol;

import io.github.jstadeusiak.spine.protocol.capture.sbe.*;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Owner-scoped capture flyweights. Ordinary observations borrow exact native/raw byte regions. */
public final class CaptureCodec {
    public static final int OBSERVATION_OVERHEAD = 36, SOURCE_DEFINITION_OVERHEAD = 40, MAX_LABEL_BYTES = 4096;
    public static final long UNKNOWN_NATIVE_ID = -1;
    public final MessageHeaderDecoder header = new MessageHeaderDecoder();
    public final CaptureObservationDecoder observation = new CaptureObservationDecoder();
    public final SourceDefinitionDecoder definition = new SourceDefinitionDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CaptureObservationEncoder observationEncoder = new CaptureObservationEncoder();
    private final SourceDefinitionEncoder definitionEncoder = new SourceDefinitionEncoder();
    private final int[] labelOffsets = new int[6], labelLengths = new int[6];
    public DirectBuffer buffer;
    public int nativeOffset, nativeLength, rawOffset, rawLength;

    public int templateId(final DirectBuffer input, final int offset, final int length) {
        if (!Wire.range(input, offset, length) || length < 8 || length > Wire.MAX_PAYLOAD_BYTES) return -1;
        header.wrap(input, offset);
        return header.schemaId() == 201 && header.version() == 1 ? header.templateId() : -1;
    }

    public boolean validObservation(final DirectBuffer input, final int offset, final int length) {
        if (templateId(input, offset, length) != 2 || header.blockLength() != 20 || length < OBSERVATION_OVERHEAD) return false;
        final int nativeSize = input.getInt(offset + 28, Wire.ORDER);
        if (nativeSize < 0 || nativeSize > MAX_LABEL_BYTES || nativeSize > length - OBSERVATION_OVERHEAD) return false;
        final int rawSize = input.getInt(offset + 32 + nativeSize, Wire.ORDER);
        if (rawSize < 0 || rawSize != length - OBSERVATION_OVERHEAD - nativeSize) return false;
        observation.wrap(input, offset + 8, 20, header.version());
        if (observation.rawFrameSequence() <= 0 || observation.subeventIndex() == 0xFFFF_FFFFL
            || observation.nativeNumericId() != UNKNOWN_NATIVE_ID && nativeSize != 0) return false;
        buffer = input;
        nativeOffset = offset + 32;
        nativeLength = nativeSize;
        rawOffset = offset + OBSERVATION_OVERHEAD + nativeSize;
        rawLength = rawSize;
        return true;
    }

    public int encodeObservation(final MutableDirectBuffer dst, final int offset, final long rawFrameSequence,
        final int subeventIndex, final long nativeNumericId, final DirectBuffer nativeBytes, final int nativeByteOffset,
        final int nativeByteLength, final DirectBuffer raw, final int rawByteOffset, final int rawByteLength) {
        final int length = OBSERVATION_OVERHEAD + nativeByteLength + rawByteLength;
        Wire.require(rawFrameSequence > 0 && subeventIndex != -1 && nativeByteLength >= 0 && nativeByteLength <= MAX_LABEL_BYTES
            && (nativeNumericId == UNKNOWN_NATIVE_ID || nativeByteLength == 0)
            && (nativeByteLength == 0 || Wire.range(nativeBytes, nativeByteOffset, nativeByteLength))
            && Wire.range(raw, rawByteOffset, rawByteLength) && length <= Wire.MAX_PAYLOAD_BYTES && Wire.range(dst, offset, length));
        observationEncoder.wrapAndApplyHeader(dst, offset, headerEncoder).rawFrameSequence(rawFrameSequence)
            .subeventIndex(Integer.toUnsignedLong(subeventIndex)).nativeNumericId(nativeNumericId)
            .putNativeEventId(nativeByteLength == 0 ? dst : nativeBytes, nativeByteLength == 0 ? offset : nativeByteOffset, nativeByteLength)
            .putRawBytes(raw, rawByteOffset, rawByteLength);
        return length;
    }

    /** Cold definitions permit UTF-8 decoding/allocation; ordinary observation validation does not. */
    public boolean validDefinition(final DirectBuffer input, final int offset, final int length) {
        if (templateId(input, offset, length) != 1 || header.blockLength() != 8 || length < SOURCE_DEFINITION_OVERHEAD) return false;
        int position = offset + 16;
        for (int i = 0; i < labelOffsets.length; i++) {
            if (position > offset + length - 4) return false;
            final int size = input.getInt(position, Wire.ORDER);
            position += 4;
            if (size <= 0 || size > MAX_LABEL_BYTES || size > offset + length - position) return false;
            final byte[] bytes = new byte[size];
            input.getBytes(position, bytes);
            if (!validLabel(bytes)) return false;
            labelOffsets[i] = position;
            labelLengths[i] = size;
            position += size;
        }
        if (position != offset + length) return false;
        definition.wrap(input, offset + 8, 8, header.version());
        if (definition.syncUncertaintyNanos() < -1) return false;
        buffer = input;
        return true;
    }

    /** Labels are provisioned UTF-8 bytes in provider/product/session/collector/configuration/clock order. */
    public int encodeDefinition(final MutableDirectBuffer dst, final int offset, final long uncertainty, final byte[][] labels) {
        Wire.require(uncertainty >= -1 && labels != null && labels.length == 6);
        int length = SOURCE_DEFINITION_OVERHEAD;
        for (final byte[] label : labels) {
            Wire.require(validLabel(label));
            length += label.length;
        }
        Wire.require(length <= Wire.MAX_PAYLOAD_BYTES && Wire.range(dst, offset, length));
        definitionEncoder.wrapAndApplyHeader(dst, offset, headerEncoder).syncUncertaintyNanos(uncertainty);
        int position = offset + 16;
        for (final byte[] label : labels) {
            dst.putInt(position, label.length, Wire.ORDER);
            dst.putBytes(position + 4, label);
            position += 4 + label.length;
        }
        return length;
    }

    public int labelOffset(final int index) { return labelOffsets[index]; }
    public int labelLength(final int index) { return labelLengths[index]; }

    private static boolean validLabel(final byte[] bytes) {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_LABEL_BYTES) return false;
        try {
            return !StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString().isBlank();
        } catch (final CharacterCodingException invalid) {
            return false;
        }
    }
}
