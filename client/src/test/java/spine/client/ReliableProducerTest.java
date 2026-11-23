package spine.client;

import spine.protocol.Wire;
import spine.protocol.ApplicationLimits;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReliableProducerTest {
    private final Wire wire = new Wire();
    private final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
    private final UnsafeBuffer event = new UnsafeBuffer(new byte[Wire.MAX_EVENT_BYTES]);
    private final UnsafeBuffer receipt = new UnsafeBuffer(new byte[Wire.MAX_RESULT_BYTES]);

    @Test void exactCapturedBytesRestoreAndDuplicateReceiptRetainsOriginalCanonicalCursor() {
        try (ReliableProducer producer = new ReliableProducer(11, 17, 4)) {
            assertThat(producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64)).isTrue();
            final byte[] capture = new byte[producer.pendingLength()];
            producer.pendingBuffer().getBytes(0, capture);
            try (ReliableProducer restarted = new ReliableProducer(11, 17, 4)) {
                assertThat(restarted.restoreExact(new UnsafeBuffer(capture), 0, capture.length)).isTrue();
                final int length = receipt(17, 4, 31, AdmissionStatus.DUPLICATE, 64);
                restarted.onReceipt(receipt, 0, length);
                assertThat(restarted.acknowledgedSequence()).isEqualTo(31);
                assertThat(restarted.nextSequence()).isEqualTo(5);
                assertThat(restarted.pendingLength()).isZero();
                assertThat(restarted.lastStatus()).isEqualTo(AdmissionStatus.DUPLICATE);
            }
        }
    }

    @Test void rejectionRetainsExactPendingBytesAndDoesNotAdvanceSequence() {
        try (ReliableProducer producer = new ReliableProducer(11, 17, 4)) {
            producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64);
            final int length = wire.encodeResult(receipt, 0, 17, 4, AdmissionStatus.GAP, event, 0, 0);
            producer.onReceipt(receipt, 0, length);
            assertThat(producer.pendingLength()).isEqualTo(Wire.PROPOSAL_OVERHEAD + 64);
            assertThat(producer.nextSequence()).isEqualTo(4);
            assertThat(producer.lastStatus()).isEqualTo(AdmissionStatus.GAP);
            assertThat(producer.failureReason()).isNull();
            assertThat(producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64)).isFalse();
        }
    }

    @Test void delayedOldAndOtherProducerReceiptsAreIgnoredButFutureOrChangedPayloadStops() {
        try (ReliableProducer producer = new ReliableProducer(11, 17, 4)) {
            producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64);
            producer.onReceipt(receipt, 0, receipt(17, 3, 29, AdmissionStatus.ACCEPTED, 64));
            producer.onReceipt(receipt, 0, receipt(19, 4, 30, AdmissionStatus.ACCEPTED, 64));
            assertThat(producer.failureReason()).isNull();
            assertThat(producer.pendingLength()).isPositive();
            final int length = receipt(17, 4, 31, AdmissionStatus.ACCEPTED, 64);
            receipt.putByte(Wire.RESULT_OVERHEAD + Wire.EVENT_OVERHEAD, (byte)1);
            producer.onReceipt(receipt, 0, length);
            assertThat(producer.failureReason()).isNotNull();
            assertThat(producer.nextSequence()).isEqualTo(4);
            assertThat(producer.pendingLength()).isPositive();
        }
        try (ReliableProducer producer = new ReliableProducer(11, 17, 4)) {
            producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64);
            producer.onReceipt(receipt, 0, receipt(17, 5, 31, AdmissionStatus.ACCEPTED, 64));
            assertThat(producer.failureReason()).isNotNull();
        }
    }

    @Test void exhaustionAndUnsignedIdentifiersDoNotWrapOrBox() {
        try (ReliableProducer producer = new ReliableProducer(Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE)) {
            assertThat(producer.submit(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, -1, -1, -1,
                payload, 0, 8)).isTrue();
            final int eventLength = wire.encodeEvent(event, 0, Long.MIN_VALUE, 1, 2, Integer.MIN_VALUE,
                Integer.MIN_VALUE, Integer.MIN_VALUE, -1, -1, -1, payload, 0, 8);
            final int length = wire.encodeResult(receipt, 0, Long.MIN_VALUE + 1, Long.MAX_VALUE,
                AdmissionStatus.ACCEPTED, event, 0, eventLength);
            producer.onReceipt(receipt, 0, length);
            assertThat(producer.nextSequence()).isZero();
            producer.onReceipt(receipt, 0, length);
            assertThat(producer.failureReason()).isNull();
            assertThat(producer.submit(1, 1, 1, -1, -1, -1, payload, 0, 8)).isFalse();
        }
    }

    @Test void applicationReceiptsAcknowledgeOnlyTheExactOriginalObservation() {
        for (final AdmissionStatus status : new AdmissionStatus[]{AdmissionStatus.ACCEPTED, AdmissionStatus.DUPLICATE}) {
            for (final int prefixLength : new int[]{8, 16, Wire.MAX_APPLICATION_PREFIX_BYTES}) {
                try (ReliableProducer producer = new ReliableProducer(11, 17, 4)) {
                    producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64);
                    producer.onReceipt(receipt, 0, applicationReceipt(status, prefixLength));
                    assertThat(producer.failureReason()).isNull();
                    assertThat(producer.pendingLength()).isZero();
                    assertThat(producer.acknowledgedSequence()).isEqualTo(31);
                    assertThat(producer.nextSequence()).isEqualTo(5);
                }
            }
        }
        for (final int prefixLength : new int[]{8, 16, ApplicationLimits.DEFAULT.resultBytes()}) {
            for (final int changed : new int[]{0, prefixLength, prefixLength + Integer.BYTES}) {
                try (ReliableProducer producer = new ReliableProducer(11, 17, 4)) {
                    producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64);
                    final int length = applicationReceipt(AdmissionStatus.ACCEPTED, prefixLength);
                    final int position = Wire.RESULT_OVERHEAD + Wire.EVENT_OVERHEAD + changed;
                    receipt.putByte(position, (byte)(receipt.getByte(position) ^ 1));
                    producer.onReceipt(receipt, 0, length);
                    assertThat(producer.failureReason()).isNotNull();
                    assertThat(producer.pendingLength()).isPositive();
                    assertThat(producer.nextSequence()).isEqualTo(4);
                }
            }
        }
    }

    @Test void variableBatchReceiptAcknowledgesExactCapturedProposalAfterRestart() {
        final int count = 32;
        final int prefixLength = 20 + count * 16;
        final UnsafeBuffer batch = new UnsafeBuffer(new byte[prefixLength + Integer.BYTES + 64]);
        batch.putShort(0, (short)8, Wire.ORDER);
        batch.putShort(2, (short)19, Wire.ORDER);
        batch.putShort(4, (short)777, Wire.ORDER);
        batch.putShort(6, (short)1, Wire.ORDER);
        batch.putLong(8, 31, Wire.ORDER);
        batch.putShort(16, (short)16, Wire.ORDER);
        batch.putShort(18, (short)count, Wire.ORDER);
        for (int i = 0; i < count; i++) {
            batch.putLong(20 + i * 16, i + 1, Wire.ORDER);
            batch.putLong(28 + i * 16, 100 + i, Wire.ORDER);
        }
        batch.putInt(prefixLength, 64, Wire.ORDER);
        batch.putBytes(prefixLength + Integer.BYTES, payload, 0, 64);
        try (ReliableProducer original = new ReliableProducer(11, 17, 4);
             ReliableProducer restored = new ReliableProducer(11, 17, 4)) {
            assertThat(original.submit(3, 5, 7, 100, 101, 102, payload, 0, 64)).isTrue();
            assertThat(restored.restoreExact(original.pendingBuffer(), 0, original.pendingLength())).isTrue();
            final int eventLength = wire.encodeEvent(event, 0, 11, 31, 500, 3, 5, 7, 100, 101, 102,
                batch, 0, batch.capacity());
            final int length = wire.encodeResult(receipt, 0, 17, 4, AdmissionStatus.DUPLICATE, event, 0, eventLength);
            restored.onReceipt(receipt, 0, length);
            assertThat(restored.failureReason()).isNull();
            assertThat(restored.pendingLength()).isZero();
            assertThat(restored.acknowledgedSequence()).isEqualTo(31);
            assertThat(restored.nextSequence()).isEqualTo(5);
            assertThat(restored.lastStatus()).isEqualTo(AdmissionStatus.DUPLICATE);
        }
    }

    @Test void applicationResultExceedingTheDeclaredPrefixBoundCannotAcknowledgePendingInput() {
        try (ReliableProducer producer = new ReliableProducer(11, 17, 4)) {
            producer.submit(3, 5, 7, 100, 101, 102, payload, 0, 64);
            producer.onReceipt(receipt, 0, applicationReceipt(AdmissionStatus.ACCEPTED, Wire.MAX_APPLICATION_PREFIX_BYTES + 1));
            assertThat(producer.failureReason()).isNotNull();
            assertThat(producer.pendingLength()).isPositive();
            assertThat(producer.nextSequence()).isEqualTo(4);
        }
    }

    private int applicationReceipt(final AdmissionStatus status, final int prefixLength) {
        final UnsafeBuffer wrapped = new UnsafeBuffer(new byte[prefixLength + Integer.BYTES + 64]);
        wrapped.putShort(0, (short)Math.min(prefixLength - Wire.HEADER_LENGTH, 65535), Wire.ORDER);
        wrapped.putShort(2, (short)19, Wire.ORDER);
        wrapped.putShort(4, (short)777, Wire.ORDER);
        wrapped.putShort(6, (short)3, Wire.ORDER);
        wrapped.putInt(prefixLength, 64, Wire.ORDER);
        wrapped.putBytes(prefixLength + Integer.BYTES, payload, 0, 64);
        final int eventLength = wire.encodeEvent(event, 0, 11, 31, 500, 3, 5, 7, 100, 101, 102,
            wrapped, 0, wrapped.capacity());
        return wire.encodeResult(receipt, 0, 17, 4, status, event, 0, eventLength);
    }

    private int receipt(final long producer, final long producerSequence, final long sequence,
                        final AdmissionStatus status, final int payloadLength) {
        final int eventLength = wire.encodeEvent(event, 0, 11, sequence, 500, 3, 5, 7, 100, 101, 102,
            payload, 0, payloadLength);
        return wire.encodeResult(receipt, 0, producer, producerSequence, status, event, 0, eventLength);
    }
}
