package io.github.jstadeusiak.spine.protocol;

import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.*;

class WireTest {
    private final Wire wire = new Wire();
    private final UnsafeBuffer dst = new UnsafeBuffer(new byte[Wire.MAX_RESULT_BYTES + 32]);
    private final UnsafeBuffer payload = inner(10);

    @Test void compactWirePreservesUnsignedIdentifiersMissingTimesAndBorrowedPayload() {
        final int length = wire.encodeProposal(dst, 7, Long.MIN_VALUE, -2, Long.MAX_VALUE,
            Integer.MIN_VALUE, -2, 0, -1, 17, 12, payload, 0, payload.capacity());
        assertThat(length).isEqualTo(72 + payload.capacity());
        assertThat(wire.validProposal(dst, 7, length)).isTrue();
        assertThat(wire.proposal.logId()).isEqualTo(Long.MIN_VALUE);
        assertThat(wire.proposal.producerId()).isEqualTo(-2);
        assertThat(wire.proposal.producerSequence()).isEqualTo(Long.MAX_VALUE);
        assertThat(wire.proposal.sourceId()).isEqualTo(2_147_483_648L);
        assertThat(wire.proposal.sourceConfigVersion()).isEqualTo(4_294_967_294L);
        assertThat(wire.proposal.receiveTimeNanos()).isEqualTo(-1);
        assertThat(wire.proposal.completeTimeNanos()).isEqualTo(17);
        assertThat(wire.proposal.readyTimeNanos()).isEqualTo(12);
        assertThat(dst.getByte(7 + Wire.PROPOSAL_OVERHEAD + 8)).isEqualTo(payload.getByte(8));
        dst.putByte(7 + Wire.PROPOSAL_OVERHEAD + 8, (byte) 93);
        assertThat(wire.proposal.buffer().getByte(7 + Wire.PROPOSAL_OVERHEAD + 8)).isEqualTo((byte) 93);
    }

    @Test void eventAndCorrelatedResultsHaveExactLengths() {
        final UnsafeBuffer event = new UnsafeBuffer(new byte[Wire.MAX_EVENT_BYTES]);
        final int eventLength = wire.encodeEvent(event, 0, 3, 9, 123, 4, 5, 6, 10, 11, 12, payload, 0, 10);
        assertThat(eventLength).isEqualTo(82);
        for (final AdmissionStatus status : new AdmissionStatus[]{AdmissionStatus.ACCEPTED, AdmissionStatus.DUPLICATE}) {
            final int length = wire.encodeResult(dst, 5, 2, 8, status, event, 0, eventLength);
            assertThat(length).isEqualTo(29 + eventLength);
            assertThat(wire.validResult(dst, 5, length)).isTrue();
            assertThat(wire.result.status()).isEqualTo(status);
            assertThat(wire.event.logId()).isEqualTo(3);
            assertThat(wire.event.sequence()).isEqualTo(9);
            assertThat(wire.event.sequencedTimeNanos()).isEqualTo(123);
        }
        for (final AdmissionStatus status : AdmissionStatus.values()) {
            if (status == AdmissionStatus.ACCEPTED || status == AdmissionStatus.DUPLICATE || status == AdmissionStatus.NULL_VAL) continue;
            assertThat(wire.encodeResult(dst, 0, 0, 0, status, null, 0, 0)).isEqualTo(29);
            assertThat(wire.validResult(dst, 0, 29)).isTrue();
        }
    }

    @Test void maximumPayloadFitsWithoutTruncation() {
        final UnsafeBuffer maximum = inner(Wire.MAX_EVENT_PAYLOAD_BYTES);
        final int length = wire.encodeEvent(dst, 0, 1, 1, -1, 0, 0, 0, -1, -1, -1,
            maximum, 0, maximum.capacity());
        assertThat(length).isEqualTo(Wire.MAX_EVENT_BYTES);
        assertThat(wire.validEvent(dst, 0, length)).isTrue();
        assertThat(dst.getByte(length - 1)).isEqualTo(maximum.getByte(maximum.capacity() - 1));
        assertThatThrownBy(() -> wire.encodeEvent(dst, 0, 1, 1, -1, 0, 0, 0, -1, -1, -1,
            inner(Wire.MAX_EVENT_PAYLOAD_BYTES + 1), 0, Wire.MAX_EVENT_PAYLOAD_BYTES + 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void rejectReservedIdsSequencesMetadataHeadersAndLengthsWithoutThrowing() {
        final int length = wire.encodeProposal(dst, 0, 1, 2, 3, 4, 5, 6, -1, -1, -1, payload, 0, 10);
        for (final int field : new int[]{8, 16, 24}) {
            final long original = dst.getLong(field, Wire.ORDER);
            for (final long invalid : new long[]{0, -1}) {
                dst.putLong(field, invalid, Wire.ORDER);
                assertThat(wire.validProposal(dst, 0, length)).isFalse();
            }
            dst.putLong(field, original, Wire.ORDER);
        }
        for (final int field : new int[]{32, 36, 40}) {
            final int original = dst.getInt(field, Wire.ORDER);
            dst.putInt(field, -1, Wire.ORDER);
            assertThat(wire.validProposal(dst, 0, length)).isFalse();
            dst.putInt(field, original, Wire.ORDER);
        }
        for (final int field : new int[]{44, 52, 60}) {
            dst.putLong(field, -2, Wire.ORDER);
            assertThat(wire.validProposal(dst, 0, length)).isFalse();
            dst.putLong(field, -1, Wire.ORDER);
        }
        assertThat(wire.validProposal(dst, 0, length - 1)).isFalse();
        assertThat(wire.validProposal(dst, 0, length + 1)).isFalse();
        dst.putInt(68, -1, Wire.ORDER);
        assertThat(wire.validProposal(dst, 0, length)).isFalse();
        dst.putInt(68, 10, Wire.ORDER);
        dst.putShort(0, (short) 59, Wire.ORDER);
        assertThat(wire.validProposal(dst, 0, length)).isFalse();
        dst.putShort(0, (short) 60, Wire.ORDER);
        dst.putShort(4, (short) 100, Wire.ORDER);
        assertThat(wire.validProposal(dst, 0, length)).isFalse();
        dst.putShort(4, (short) 101, Wire.ORDER);
        dst.putShort(6, (short) 2, Wire.ORDER);
        assertThat(wire.validProposal(dst, 0, length)).isFalse();
        dst.putShort(6, (short) 1, Wire.ORDER);
        dst.putShort(72, (short) 3, Wire.ORDER);
        assertThat(wire.validProposal(dst, 0, length)).isFalse();
    }

    @Test void malformedStatusAndResultShapeAreRejectedBeforeEnumDecoding() {
        wire.encodeResult(dst, 0, 2, 3, AdmissionStatus.GAP, null, 0, 0);
        dst.putByte(24, (byte) 255);
        assertThat(wire.validResult(dst, 0, 29)).isFalse();
        dst.putByte(24, (byte) 11);
        assertThat(wire.validResult(dst, 0, 29)).isFalse();
        dst.putByte(24, (byte) AdmissionStatus.ACCEPTED.value());
        assertThat(wire.validResult(dst, 0, 29)).isFalse();
        dst.putByte(24, (byte) AdmissionStatus.GAP.value());
        dst.putLong(8, 0, Wire.ORDER);
        assertThat(wire.validResult(dst, 0, 29)).isFalse();
    }

    @Test void initializationHeadAndSnapshotBoundariesAreCompact() {
        assertThat(wire.encodeInitialize(dst, 3, -2)).isEqualTo(Wire.INITIALIZE_LENGTH);
        assertThat(wire.validInitialize(dst, 3, Wire.INITIALIZE_LENGTH)).isTrue();
        assertThat(wire.initialize.logId()).isEqualTo(-2);
        assertThat(wire.validInitialize(dst, 3, Wire.INITIALIZE_LENGTH + 1)).isFalse();
        assertThat(wire.encodeHead(dst, 0, 1, 0, 7, 42, 9, 13, 64, 1)).isEqualTo(64);
        assertThat(wire.validHead(dst, 0, 64)).isTrue();
        assertThat(wire.head.recordingId()).isEqualTo(9);
        wire.snapshotEncoder.wrapAndApplyHeader(dst, 0, wire.headerEncoder).logId(0).sequence(0)
            .applicationId(1).applicationVersion(1).configurationId(1).receiptsCount(0);
        wire.snapshotEncoder.putApplicationState(dst, 0, 0);
        assertThat(wire.validSnapshot(dst, 0, Wire.SNAPSHOT_OVERHEAD + 4)).isTrue();
        dst.putLong(16, 1, Wire.ORDER);
        assertThat(wire.validSnapshot(dst, 0, Wire.SNAPSHOT_OVERHEAD + 4)).isFalse();
        wire.snapshotEncoder.wrapAndApplyHeader(dst, 0, wire.headerEncoder).logId(1).sequence(1)
            .applicationId(1).applicationVersion(1).configurationId(1).receiptsCount(1)
            .next().producerId(2).producerSequence(1).canonicalSequence(1).sequencedTimeNanos(-1)
            .putDigest(new byte[32], 0, 32).putApplicationResult(dst, 0, 0);
        wire.snapshotEncoder.putApplicationState(dst, 0, 0);
        assertThat(wire.validSnapshot(dst, 0, Wire.SNAPSHOT_OVERHEAD + Wire.RECEIPT_OVERHEAD + 4)).isTrue();
        assertThat(wire.validSnapshot(dst, 0, Wire.SNAPSHOT_OVERHEAD + Wire.RECEIPT_OVERHEAD + 3)).isFalse();
        dst.putShort(68, (short) 63, Wire.ORDER);
        assertThat(wire.validSnapshot(dst, 0, Wire.SNAPSHOT_OVERHEAD + Wire.RECEIPT_OVERHEAD + 4)).isFalse();
    }

    @Test void applicationPrefixSupportsBoundedSbeGroupsAndVariableData() {
        assertThat(Wire.validApplicationPrefix(dst, 3, 0)).isTrue();
        for (final int length : new int[]{8, 16, Wire.MAX_APPLICATION_PREFIX_BYTES}) {
            dst.putShort(3, (short)0, Wire.ORDER);
            assertThat(Wire.validApplicationPrefix(dst, 3, length)).isTrue();
        }
        dst.putShort(3, (short)9, Wire.ORDER);
        assertThat(Wire.validApplicationPrefix(dst, 3, 16)).isFalse();
        assertThat(Wire.validApplicationPrefix(dst, 3, -1)).isFalse();
        assertThat(Wire.validApplicationPrefix(dst, 3, 7)).isFalse();
        assertThat(Wire.validApplicationPrefix(dst, 3, Wire.MAX_APPLICATION_PREFIX_BYTES + 1)).isFalse();
        assertThat(Wire.validApplicationPrefix(dst, dst.capacity() - 7, 8)).isFalse();
        assertThat(Wire.validApplicationPrefix(dst, Integer.MAX_VALUE, 8)).isFalse();
    }

    @Test void snapshotWalksExactVariableReceiptLengthsAndRejectsTruncationBeforeReading() {
        final int offset = 13;
        final UnsafeBuffer result = inner(131_072);
        final var encoder = wire.snapshotEncoder.wrapAndApplyHeader(dst, offset, wire.headerEncoder)
            .logId(1).sequence(2).applicationId(1).applicationVersion(1).configurationId(1);
        final var receipts = encoder.receiptsCount(2);
        receipts.next().producerId(2).producerSequence(1).canonicalSequence(1).sequencedTimeNanos(1)
            .putDigest(new byte[32], 0, 32).putApplicationResult(result, 0, result.capacity());
        receipts.next().producerId(3).producerSequence(1).canonicalSequence(2).sequencedTimeNanos(2)
            .putDigest(new byte[32], 0, 32).putApplicationResult(result, 0, 8);
        encoder.putApplicationState(result, 0, 8);
        final int length = 8 + encoder.encodedLength();
        assertThat(wire.validSnapshot(dst, offset, length)).isTrue();
        assertThat(wire.applicationStateOffset).isEqualTo(offset + length - 12);
        for (int cut = 0; cut < length; cut++) {
            assertThat(wire.validSnapshot(dst, offset, cut)).as("truncated length %s", cut).isFalse();
        }
        final int resultLengthOffset = offset + Wire.SNAPSHOT_OVERHEAD + 64;
        for (final int invalid : new int[]{-1, Integer.MAX_VALUE, Wire.MAX_APPLICATION_PREFIX_BYTES + 1}) {
            dst.putInt(resultLengthOffset, invalid, Wire.ORDER);
            assertThat(wire.validSnapshot(dst, offset, length)).isFalse();
        }
    }

    @Test void arbitraryBytesAndInvalidRangesNeverEscapeAsExceptions() {
        final Random random = new Random(9);
        final byte[] bytes = new byte[128];
        final UnsafeBuffer input = new UnsafeBuffer(bytes);
        for (int i = 0; i < 10000; i++) {
            random.nextBytes(bytes);
            final int offset = random.nextInt(132) - 2;
            final int length = random.nextInt(132) - 2;
            wire.templateId(input, offset, length);
            wire.validProposal(input, offset, length);
            wire.validEvent(input, offset, length);
            wire.validInitialize(input, offset, length);
            wire.validResult(input, offset, length);
            wire.validSnapshot(input, offset, length);
            wire.validHead(input, offset, length);
            Wire.validApplicationPrefix(input, offset, length);
        }
        assertThat(wire.templateId(null, 0, 8)).isEqualTo(-1);
        assertThat(wire.validProposal(input, Integer.MAX_VALUE, Integer.MAX_VALUE)).isFalse();
    }

    private static UnsafeBuffer inner(final int size) {
        final UnsafeBuffer value = new UnsafeBuffer(new byte[size]);
        value.putShort(0, (short) 0, Wire.ORDER);
        value.putShort(2, (short) 17, Wire.ORDER);
        value.putShort(4, (short) 777, Wire.ORDER);
        value.putShort(6, (short) 12, Wire.ORDER);
        for (int i = 8; i < size; i++) value.putByte(i, (byte) i);
        return value;
    }
}
