package io.github.jstadeusiak.spine.sequencer;

import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import io.github.jstadeusiak.spine.protocol.sbe.SequencerOutputSnapshotEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SequencerCoreTest {
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    private static final long LOG = 11;
    private static final long PRODUCER = 21;
    private final Wire wire = new Wire();
    private final SequencerCore core = new SequencerCore();

    @Test
    void initializationIsExplicitIdempotentAndCannotChangeAnExistingHistory() {
        final UnsafeBuffer proposal = proposal(LOG, PRODUCER, 1, 7);
        assertThat(send(proposal, 1)).isEqualTo(AdmissionStatus.UNINITIALIZED);
        assertThat(core.sequence()).isZero();
        assertThat(send(initialize(LOG), 2)).isEqualTo(AdmissionStatus.INITIALIZED);
        assertThat(send(initialize(LOG), 3)).isEqualTo(AdmissionStatus.INITIALIZED);
        assertThat(send(initialize(99), 4)).isEqualTo(AdmissionStatus.LOG_CONFLICT);
        assertThat(core.logId()).isEqualTo(LOG);
        assertThat(send(proposal, 5)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(core.sequence()).isEqualTo(1);
        assertThat(core.eventLength()).isEqualTo(Wire.EVENT_OVERHEAD + 16);
        assertThat(core.eventOffset()).isEqualTo(Wire.RESULT_OVERHEAD);
    }

    @Test
    void aWrongLogDoesNotConsumeAProducerSequenceOrResurrectAnUninitializedHistory() {
        assertThat(send(proposal(99, PRODUCER, 1, 7), 1)).isEqualTo(AdmissionStatus.UNINITIALIZED);
        send(initialize(LOG), 2);
        assertThat(send(proposal(99, PRODUCER, 1, 7), 3)).isEqualTo(AdmissionStatus.LOG_CONFLICT);
        assertResult(PRODUCER, 1, AdmissionStatus.LOG_CONFLICT);
        assertThat(core.sequence()).isZero();
        assertThat(send(proposal(LOG, PRODUCER, 1, 7), 4)).isEqualTo(AdmissionStatus.ACCEPTED);
    }

    @Test
    void latestRetriesReturnTheOriginalCanonicalBytesWithoutEmittingAgain() {
        send(initialize(LOG), 1);
        final UnsafeBuffer first = proposal(LOG, PRODUCER, 1, 7);
        assertThat(send(first, 2)).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] original = receiptEvent();
        assertThat(send(first, 999)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(core.eventLength()).isZero();
        assertThat(receiptEvent()).isEqualTo(original);
        assertThat(core.sequence()).isEqualTo(1);
        assertResult(PRODUCER, 1, AdmissionStatus.DUPLICATE);
        assertThat(wire.validEvent(core.resultBuffer(), Wire.RESULT_OVERHEAD,
            core.resultLength() - Wire.RESULT_OVERHEAD)).isTrue();
        assertThat(wire.event.sequencedTimeNanos()).isEqualTo(2_000_000);
        assertThat(send(proposal(LOG, PRODUCER, 2, 8), 3)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(send(first, 4)).isEqualTo(AdmissionStatus.EXPIRED);
        assertThat(core.sequence()).isEqualTo(2);
    }

    @Test
    void exactRetryHashesEveryIngressByteAndPreservesMetadataAndRawPayload() {
        send(initialize(LOG), 1);
        final UnsafeBuffer first = proposal(LOG, PRODUCER, 1, 7);
        send(first, 2);
        final byte[] expected = receiptEvent();
        for (final int changedOffset : new int[] { 32, 36, 40, 44, 52, 60, 80 }) {
            final UnsafeBuffer changed = copy(first);
            changed.putByte(changedOffset, (byte)(changed.getByte(changedOffset) + 1));
            assertThat(send(changed, 3)).isEqualTo(AdmissionStatus.CONFLICT);
            assertThat(core.sequence()).isEqualTo(1);
            assertThat(core.resultLength()).isEqualTo(Wire.RESULT_OVERHEAD);
        }
        assertThat(send(first, 4)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(receiptEvent()).isEqualTo(expected);
    }

    @Test
    void unrelatedProducersHaveIndependentContiguousProgressAndOneCanonicalOrder() {
        send(initialize(LOG), 1);
        assertThat(send(proposal(LOG, PRODUCER, 2, 8), 2)).isEqualTo(AdmissionStatus.GAP);
        assertThat(send(proposal(LOG, PRODUCER, 1, 7), 3)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(send(proposal(LOG, 22, 1, 8), 4)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(send(proposal(LOG, PRODUCER, 3, 9), 5)).isEqualTo(AdmissionStatus.GAP);
        assertThat(send(proposal(LOG, PRODUCER, 2, 9), 6)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(core.sequence()).isEqualTo(3);
        assertThat(send(proposal(LOG, 22, 1, 8), 7)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(wire.validEvent(core.resultBuffer(), Wire.RESULT_OVERHEAD,
            core.resultLength() - Wire.RESULT_OVERHEAD)).isTrue();
        assertThat(wire.event.sequence()).isEqualTo(2);
    }

    @Test
    void loggedTimeIsConvertedExactlyBeforeAdmissionAndOriginalObservationsAreUntouched() {
        send(initialize(LOG), 1);
        final UnsafeBuffer input = proposal(LOG, PRODUCER, 1, 7);
        for (final long[] invalid : new long[][] { { -1, 1 }, { Long.MAX_VALUE, 2 }, { 1, 0 }, { 1, -1 } }) {
            assertThat(core.onCommand(input, 0, input.capacity(), invalid[0], invalid[1]))
                .isEqualTo(AdmissionStatus.INVALID);
            assertThat(core.sequence()).isZero();
        }
        assertThat(core.onCommand(input, 0, input.capacity(), Long.MAX_VALUE, 1)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(wire.validEvent(core.resultBuffer(), core.eventOffset(), core.eventLength())).isTrue();
        assertThat(wire.event.sequencedTimeNanos()).isEqualTo(Long.MAX_VALUE);
        assertThat(wire.event.sourceId()).isEqualTo(1);
        assertThat(wire.event.sourceConfigVersion()).isEqualTo(2);
        assertThat(wire.event.clockId()).isEqualTo(3);
        assertThat(wire.event.receiveTimeNanos()).isEqualTo(10);
        assertThat(wire.event.completeTimeNanos()).isEqualTo(11);
        assertThat(wire.event.readyTimeNanos()).isEqualTo(12);
        assertThat(core.onCommand(input, 0, input.capacity(), -1, 0)).isEqualTo(AdmissionStatus.DUPLICATE);
    }

    @Test
    void memoryCapacityIsExplicitWithoutEvictingLatestReceipts() {
        send(initialize(LOG), 1);
        for (int i = 1; i <= Wire.MAX_PRODUCERS; i++) {
            assertThat(send(proposal(LOG, i, 1, i), i)).isEqualTo(AdmissionStatus.ACCEPTED);
        }
        assertThat(send(proposal(LOG, 10_000, 1, 7), 1)).isEqualTo(AdmissionStatus.CAPACITY);
        assertThat(core.sequence()).isEqualTo(Wire.MAX_PRODUCERS);
        assertThat(send(proposal(LOG, 1, 1, 1), 2)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(send(proposal(LOG, 1, 2, 2), 3)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(core.sequence()).isEqualTo(Wire.MAX_PRODUCERS + 1);
        assertThat(core.snapshot()).isEqualTo(Wire.SNAPSHOT_OVERHEAD + Wire.MAX_PRODUCERS * Wire.RECEIPT_OVERHEAD
            + 4);
    }

    @Test
    void fullUnsignedIdentityBitsRemainDistinctAndUnsignedSourceIdsArePreserved() {
        final long highLog = Long.MIN_VALUE;
        send(initialize(highLog), 1);
        final UnsafeBuffer first = proposal(highLog, 1, 1, 7);
        final UnsafeBuffer second = proposal(highLog, Long.MIN_VALUE, 1, 8);
        second.putInt(32, -2, ORDER);
        second.putInt(36, Integer.MIN_VALUE, ORDER);
        second.putInt(40, -2, ORDER);
        assertThat(send(first, 2)).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] original = receiptEvent();
        assertThat(send(second, 3)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(wire.validEvent(core.resultBuffer(), core.eventOffset(), core.eventLength())).isTrue();
        assertThat(wire.event.logId()).isEqualTo(highLog);
        assertThat(wire.event.sourceId()).isEqualTo(4_294_967_294L);
        assertThat(wire.event.sourceConfigVersion()).isEqualTo(2_147_483_648L);
        assertThat(wire.event.clockId()).isEqualTo(4_294_967_294L);
        assertThat(send(first, 4)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(receiptEvent()).isEqualTo(original);
        final int length = core.snapshot();
        assertThat(wire.validSnapshot(core.resultBuffer(), 0, length)).isTrue();
        final var group = wire.snapshot.receipts();
        assertThat(group.next().producerId()).isEqualTo(1);
        group.skipApplicationResult();
        assertThat(group.next().producerId()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void maximumPayloadAndNonzeroBufferOffsetsUseTheSameAdmissionAndRetryPath() {
        send(initialize(LOG), 1);
        final UnsafeBuffer input = proposal(LOG, PRODUCER, 1, payload(7, Wire.MAX_PAYLOAD_BYTES));
        final UnsafeBuffer shifted = new UnsafeBuffer(new byte[input.capacity() + 23]);
        shifted.putBytes(13, input, 0, input.capacity());
        assertThat(core.onCommand(shifted, 13, input.capacity(), 2, 1)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(core.eventLength()).isEqualTo(Wire.EVENT_OVERHEAD + Wire.MAX_PAYLOAD_BYTES);
        final byte[] original = receiptEvent();
        assertThat(core.onCommand(shifted, 13, input.capacity(), 100, 1)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(core.eventLength()).isZero();
        assertThat(receiptEvent()).isEqualTo(original);
    }

    @Test
    void unsupportedAndMalformedCommandsReturnUncorrelatedInvalidWithoutMutatingState() {
        send(initialize(LOG), 1);
        final UnsafeBuffer valid = proposal(LOG, PRODUCER, 1, 7);
        final List<Consumer<UnsafeBuffer>> corruptions = List.of(
            b -> b.putShort(0, (short)59, ORDER), b -> b.putShort(2, (short)1, ORDER),
            b -> b.putShort(4, (short)100, ORDER), b -> b.putShort(6, (short)3, ORDER),
            b -> b.putLong(8, 0, ORDER), b -> b.putLong(8, -1, ORDER),
            b -> b.putLong(16, 0, ORDER), b -> b.putLong(16, -1, ORDER),
            b -> b.putLong(24, 0, ORDER), b -> b.putLong(24, Long.MIN_VALUE, ORDER),
            b -> b.putInt(32, -1, ORDER), b -> b.putInt(36, -1, ORDER), b -> b.putInt(40, -1, ORDER),
            b -> b.putLong(44, -2, ORDER), b -> b.putLong(52, -2, ORDER), b -> b.putLong(60, -2, ORDER),
            b -> b.putInt(68, -1, ORDER), b -> b.putInt(68, 15, ORDER),
            b -> b.putShort(72, (short)17, ORDER));
        for (final Consumer<UnsafeBuffer> corruption : corruptions) {
            final UnsafeBuffer input = copy(valid);
            corruption.accept(input);
            assertThat(send(input, 2)).isEqualTo(AdmissionStatus.INVALID);
            assertResult(0, 0, AdmissionStatus.INVALID);
            assertThat(core.sequence()).isZero();
        }
        for (final int length : new int[] { -1, 0, 7, 8, 15, 71, 87, 89, Integer.MAX_VALUE }) {
            assertThat(core.onCommand(valid, 0, length, 1, 1)).isEqualTo(AdmissionStatus.INVALID);
        }
        for (final int offset : new int[] { -1, 1, Integer.MAX_VALUE }) {
            assertThat(core.onCommand(valid, offset, valid.capacity(), 1, 1)).isEqualTo(AdmissionStatus.INVALID);
        }
        assertThat(core.onCommand(null, 0, 0, 1, 1)).isEqualTo(AdmissionStatus.INVALID);
        assertThat(send(valid, 3)).isEqualTo(AdmissionStatus.ACCEPTED);
    }

    @Test
    void malformedRandomBytesNeverThrowReserveIdentityOrChangeState() {
        send(initialize(LOG), 1);
        final byte[] expected = state(core);
        final Random random = new Random(938_241);
        for (int i = 0; i < 10_000; i++) {
            final byte[] bytes = new byte[random.nextInt(200)];
            random.nextBytes(bytes);
            final UnsafeBuffer input = new UnsafeBuffer(bytes);
            assertThat(send(input, i)).isEqualTo(AdmissionStatus.INVALID);
            assertThat(core.eventLength()).isZero();
        }
        assertThat(state(core)).isEqualTo(expected);
    }

    @Test
    void replayAndSnapshotSuffixRegenerateByteIdenticalResultsEventsAndState() {
        final List<UnsafeBuffer> inputs = List.of(initialize(LOG), proposal(LOG, 1, 1, 7),
            proposal(LOG, Long.MIN_VALUE, 1, 8), proposal(LOG, 1, 1, 7), proposal(LOG, 1, 1, 9),
            proposal(LOG, 1, 3, 9), proposal(99, 1, 2, 9), proposal(LOG, 1, 2, 9),
            proposal(LOG, Long.MIN_VALUE, 2, 10), proposal(LOG, 1, 1, 7), proposal(LOG, 3, 1, 11));
        final SequencerCore twin = new SequencerCore();
        final SequencerCore restored = new SequencerCore();
        for (int i = 0; i < inputs.size(); i++) {
            final UnsafeBuffer input = inputs.get(i);
            final AdmissionStatus expected = core.onCommand(input, 0, input.capacity(), i + 1, 1_000);
            assertThat(twin.onCommand(input, 0, input.capacity(), i + 1, 1_000)).isEqualTo(expected);
            assertThat(bytes(twin.resultBuffer(), 0, twin.resultLength()))
                .isEqualTo(bytes(core.resultBuffer(), 0, core.resultLength()));
            assertThat(twin.eventLength()).isEqualTo(core.eventLength());
            if (i == 5) {
                final byte[] snapshot = state(core);
                final UnsafeBuffer shifted = new UnsafeBuffer(new byte[snapshot.length + 9]);
                shifted.putBytes(9, snapshot);
                restored.restore(shifted, 9, snapshot.length);
            } else if (i > 5) {
                assertThat(restored.onCommand(input, 0, input.capacity(), i + 1, 1_000)).isEqualTo(expected);
                assertThat(bytes(restored.resultBuffer(), 0, restored.resultLength()))
                    .isEqualTo(bytes(core.resultBuffer(), 0, core.resultLength()));
                assertThat(restored.eventLength()).isEqualTo(core.eventLength());
            }
        }
        assertThat(state(twin)).isEqualTo(state(core));
        assertThat(state(restored)).isEqualTo(state(core));
    }

    @Test
    void arbitrarySnapshotOrderRestoresToAnIdenticalCanonicalUnsignedOrder() throws Exception {
        final Receipt[] rows = new Receipt[64];
        final Receipt[] reversed = new Receipt[64];
        for (int i = 0; i < rows.length; i++) {
            final long id = i == 0 ? Long.MIN_VALUE : i;
            rows[i] = new Receipt(id, 1, i + 1, i + 1, hash(proposal(LOG, id, 1, i)));
            reversed[rows.length - i - 1] = rows[i];
        }
        final UnsafeBuffer first = snapshot(LOG, 64, rows);
        final UnsafeBuffer second = snapshot(LOG, 64, reversed);
        core.restore(first, 0, first.capacity());
        final byte[] canonical = state(core);
        core.restore(second, 0, second.capacity());
        assertThat(state(core)).isEqualTo(canonical);
        core.restore(new UnsafeBuffer(canonical), 0, canonical.length);
        assertThat(state(core)).isEqualTo(canonical);
        assertThat(send(proposal(LOG, Long.MIN_VALUE, 1, 0), 100)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(send(proposal(LOG, 63, 2, 64), 101)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(core.sequence()).isEqualTo(65);
    }

    @Test
    void snapshotReceiptsRetainTheExactRetryHashAndOriginalLoggedTimestamp() {
        send(initialize(LOG), 1);
        final UnsafeBuffer original = proposal(LOG, PRODUCER, 1, 7);
        send(original, 2);
        final byte[] event = receiptEvent();
        final byte[] saved = state(core);
        final SequencerCore fresh = new SequencerCore();
        fresh.restore(new UnsafeBuffer(saved), 0, saved.length);
        assertThat(fresh.onCommand(original, 0, original.capacity(), 100, 1)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(bytes(fresh.resultBuffer(), Wire.RESULT_OVERHEAD, fresh.resultLength() - Wire.RESULT_OVERHEAD))
            .isEqualTo(event);
        final UnsafeBuffer changed = proposal(LOG, PRODUCER, 1, 8);
        assertThat(fresh.onCommand(changed, 0, changed.capacity(), 101, 1)).isEqualTo(AdmissionStatus.CONFLICT);
        assertThat(fresh.sequence()).isEqualTo(1);
    }

    @Test
    void canonicalSequenceExhaustionRejectsBeforeWrappingWithoutReplacingReceipts() throws Exception {
        final UnsafeBuffer original = proposal(LOG, PRODUCER, Long.MAX_VALUE, 7);
        final UnsafeBuffer saved = snapshot(LOG, Long.MAX_VALUE,
            new Receipt(PRODUCER, Long.MAX_VALUE, Long.MAX_VALUE, 123, hash(original)));
        core.restore(saved, 0, saved.capacity());
        assertThat(send(proposal(LOG, 99, 1, 8), 1)).isEqualTo(AdmissionStatus.EXHAUSTED);
        assertThat(core.sequence()).isEqualTo(Long.MAX_VALUE);
        assertThat(send(original, 2)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(send(proposal(LOG, PRODUCER, Long.MAX_VALUE - 1, 7), 3)).isEqualTo(AdmissionStatus.EXPIRED);
        assertThat(state(core)).isEqualTo(saved.byteArray());
    }

    @Test
    void hostileSnapshotsFailBeforeReplacingAnyLiveState() throws Exception {
        send(initialize(LOG), 1);
        final UnsafeBuffer original = proposal(LOG, PRODUCER, 1, 7);
        send(original, 2);
        final byte[] before = state(core);
        final UnsafeBuffer valid = snapshot(99, 3,
            new Receipt(21, 2, 3, 123, hash(proposal(99, 21, 2, 8))),
            new Receipt(22, 1, 2, 122, hash(proposal(99, 22, 1, 9))));
        final int first = Wire.SNAPSHOT_OVERHEAD;
        final int second = first + Wire.RECEIPT_OVERHEAD;
        final List<Consumer<UnsafeBuffer>> corruptions = List.of(
            b -> b.putShort(0, (short)15, ORDER), b -> b.putShort(2, (short)3, ORDER),
            b -> b.putShort(4, (short)100, ORDER), b -> b.putShort(6, (short)3, ORDER),
            b -> b.putLong(8, 0, ORDER), b -> b.putLong(8, -1, ORDER),
            b -> b.putLong(16, -1, ORDER), b -> b.putLong(16, 2, ORDER), b -> b.putLong(16, 4, ORDER),
            b -> b.putShort(68, (short)63, ORDER), b -> b.putShort(70, (short)1025, ORDER),
            b -> b.putLong(first, 0, ORDER), b -> b.putLong(first, -1, ORDER),
            b -> b.putLong(first + 8, 0, ORDER), b -> b.putLong(first + 8, 4, ORDER),
            b -> b.putLong(first + 16, 0, ORDER), b -> b.putLong(first + 16, 4, ORDER),
            b -> b.putLong(first + 24, -1, ORDER), b -> b.putLong(second, 21, ORDER),
            b -> b.putLong(second + 16, 3, ORDER), b -> b.putLong(second + 8, Long.MAX_VALUE, ORDER));
        for (final Consumer<UnsafeBuffer> corruption : corruptions) {
            final UnsafeBuffer input = copy(valid);
            corruption.accept(input);
            SnapshotChecksum.seal(input);
            assertThatThrownBy(() -> core.restore(input, 0, input.capacity())).isInstanceOf(IllegalArgumentException.class);
            assertThat(state(core)).isEqualTo(before);
        }
        for (final int length : new int[] { -1, 0, 7, valid.capacity() - 1, valid.capacity() + 1, Integer.MAX_VALUE }) {
            assertThatThrownBy(() -> core.restore(valid, 0, length)).isInstanceOf(IllegalArgumentException.class);
            assertThat(state(core)).isEqualTo(before);
        }
        assertThat(send(original, 3)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(send(proposal(LOG, PRODUCER, 2, 8), 4)).isEqualTo(AdmissionStatus.ACCEPTED);
    }

    @Test
    void emptySnapshotRepresentsOnlyAnEmptyUninitializedOrInitializedHistory() {
        final UnsafeBuffer uninitialized = snapshot(0, 0);
        core.restore(uninitialized, 0, uninitialized.capacity());
        assertThat(core.logId()).isZero();
        assertThat(send(proposal(LOG, PRODUCER, 1, 7), 1)).isEqualTo(AdmissionStatus.UNINITIALIZED);
        final UnsafeBuffer initialized = snapshot(LOG, 0);
        core.restore(initialized, 0, initialized.capacity());
        assertThat(send(proposal(LOG, PRODUCER, 1, 7), 2)).isEqualTo(AdmissionStatus.ACCEPTED);
    }

    private AdmissionStatus send(final UnsafeBuffer input, final long time) {
        return core.onCommand(input, 0, input.capacity(), time, 1_000_000);
    }

    private void assertResult(final long producer, final long submitted, final AdmissionStatus status) {
        assertThat(wire.validResult(core.resultBuffer(), 0, core.resultLength())).isTrue();
        assertThat(wire.result.producerId()).isEqualTo(producer);
        assertThat(wire.result.producerSequence()).isEqualTo(submitted);
        assertThat(wire.result.status()).isEqualTo(status);
    }

    private byte[] receiptEvent() {
        return bytes(core.resultBuffer(), Wire.RESULT_OVERHEAD, core.resultLength() - Wire.RESULT_OVERHEAD);
    }

    private UnsafeBuffer initialize(final long log) {
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        wire.encodeInitialize(input, 0, log);
        return input;
    }

    private UnsafeBuffer proposal(final long log, final long producer, final long submitted, final long value) {
        return proposal(log, producer, submitted, payload(value, 16));
    }

    private UnsafeBuffer proposal(final long log, final long producer, final long submitted, final UnsafeBuffer payload) {
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payload.capacity()]);
        wire.encodeProposal(input, 0, log, producer, submitted, 1, 2, 3, 10, 11, 12, payload, 0, payload.capacity());
        return input;
    }

    private UnsafeBuffer snapshot(final long log, final long sequence, final Receipt... rows) {
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.SNAPSHOT_OVERHEAD + rows.length * Wire.RECEIPT_OVERHEAD
            + 4]);
        final SequencerOutputSnapshotEncoder encoder = wire.snapshotEncoder
            .wrapAndApplyHeader(input, 0, wire.headerEncoder).logId(log).sequence(sequence)
            .applicationId(1).applicationVersion(1).configurationId(1);
        final var group = encoder.receiptsCount(rows.length);
        for (final Receipt row : rows) {
            group.next().producerId(row.producer).producerSequence(row.submitted).canonicalSequence(row.canonical)
                .sequencedTimeNanos(row.time).putDigest(row.hash, 0, 32)
                .putApplicationResult(input, 0, 0);
        }
        encoder.putApplicationState(input, 0, 0);
        SnapshotChecksum.seal(input);
        return input;
    }

    static UnsafeBuffer payload(final long value, final int length) {
        final UnsafeBuffer bytes = new UnsafeBuffer(new byte[length]);
        bytes.putShort(0, (short)(length - 8), ORDER);
        bytes.putShort(2, (short)1, ORDER);
        bytes.putShort(4, (short)999, ORDER);
        bytes.putLong(8, value, ORDER);
        return bytes;
    }

    static byte[] bytes(final DirectBuffer input, final int offset, final int length) {
        final byte[] bytes = new byte[length];
        input.getBytes(offset, bytes);
        return bytes;
    }

    private static byte[] state(final SequencerCore target) {
        final int length = target.snapshot();
        return bytes(target.resultBuffer(), 0, length);
    }

    private static UnsafeBuffer copy(final UnsafeBuffer input) {
        return new UnsafeBuffer(input.byteArray().clone());
    }

    private static byte[] hash(final UnsafeBuffer input) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(input.byteArray());
    }

    private record Receipt(long producer, long submitted, long canonical, long time, byte[] hash) { }
}
