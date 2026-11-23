package spine.sequencer;

import spine.protocol.Wire;
import spine.protocol.ApplicationIdentity;
import spine.protocol.ApplicationLimits;
import spine.protocol.sbe.AdmissionStatus;
import spine.protocol.sbe.InitializeLogDecoder;
import spine.protocol.sbe.ObservationProposalDecoder;
import spine.protocol.sbe.SequencedEventEncoder;
import spine.protocol.sbe.SequencerOutputSnapshotDecoder;
import spine.protocol.sbe.SequencerOutputSnapshotEncoder;
import org.agrona.DirectBuffer;
import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.security.DigestException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;

/** One owner. Inputs, results and snapshots are borrowed; consume results before the next call. */
public final class SequencerCore {
    private final Wire wire = new Wire();
    private final Long2LongHashMap producers;
    private final long[] ids, submitted, canonical, times;
    private final int[] order, applicationLengths;
    private final byte[] hashes, applicationResults;
    private final byte[] ingress = new byte[Wire.MAX_PROPOSAL_BYTES];
    private final byte[] hash = new byte[32];
    private final UnsafeBuffer output;
    private final UnsafeBuffer prefix = new UnsafeBuffer();
    private Application application;
    private final ApplicationIdentity identity;
    private final ApplicationLimits limits;
    private final MessageDigest digest;
    private long logId;
    private long sequence;
    private int count;
    private int resultLength;
    private int eventLength;

    public SequencerCore() { this(ApplicationIdentity.PASS_THROUGH, Application.PASS_THROUGH); }

    public SequencerCore(final ApplicationIdentity identity, final Application application) {
        this(identity, application, ApplicationLimits.DEFAULT);
    }

    public SequencerCore(final ApplicationIdentity identity, final Application application, final ApplicationLimits limits) {
        this.identity = java.util.Objects.requireNonNull(identity);
        this.application = java.util.Objects.requireNonNull(application);
        this.limits = java.util.Objects.requireNonNull(limits);
        producers = new Long2LongHashMap(limits.producers() * 2, Hashing.DEFAULT_LOAD_FACTOR, -1L);
        ids = new long[limits.producers()];
        submitted = new long[limits.producers()];
        canonical = new long[limits.producers()];
        times = new long[limits.producers()];
        order = new int[limits.producers()];
        applicationLengths = new int[limits.producers()];
        hashes = new byte[limits.producers() * 32];
        applicationResults = new byte[limits.producers() * limits.resultBytes()];
        output = new UnsafeBuffer(new byte[Math.max(limits.snapshotBytes(), Wire.RESULT_OVERHEAD + limits.eventBytes())]);
        try {
            digest = MessageDigest.getInstance("SHA-256", "SUN");
            digest.update(ingress, 0, 128);
            digest.digest(hash, 0, hash.length);
        } catch (final GeneralSecurityException ex) {
            throw new IllegalStateException("SHA-256 SUN provider unavailable", ex);
        }
    }

    /**
     * Apply one committed command on the core thread. Supply the Cluster timestamp and the number of
     * nanoseconds in its unit. The receipt starts at offset zero in {@link #resultBuffer()}. A positive
     * {@link #eventLength()} identifies a new event to publish. A duplicate carries its event inside the
     * receipt and leaves the publication length at zero. Consume both slices before the next core operation.
     */
    public AdmissionStatus onCommand(final DirectBuffer input, final int offset, final int length,
        final long loggedTime, final long nanosPerTimeUnit) {
        eventLength = 0;
        final int template = wire.templateId(input, offset, length);
        if (template == InitializeLogDecoder.TEMPLATE_ID && wire.validInitialize(input, offset, length)) {
            if (!identity.matches(wire.initialize.applicationId(), wire.initialize.applicationVersion(),
                wire.initialize.configurationId())) {
                throw new IllegalStateException("committed application identity mismatch");
            }
            final long requested = wire.initialize.logId();
            if (logId == 0) {
                logId = requested;
            }
            return reply(0, 0, logId == requested ? AdmissionStatus.INITIALIZED : AdmissionStatus.LOG_CONFLICT, 0);
        }
        if (template != ObservationProposalDecoder.TEMPLATE_ID || !wire.validProposal(input, offset, length)) {
            return reply(0, 0, AdmissionStatus.INVALID, 0);
        }
        final ObservationProposalDecoder proposal = wire.proposal;
        final long producer = proposal.producerId();
        final long next = proposal.producerSequence();
        final int index = (int)producers.get(producer);
        if (logId == 0) {
            return reply(producer, next, AdmissionStatus.UNINITIALIZED, 0);
        }
        if (proposal.logId() != logId) {
            return reply(producer, next, AdmissionStatus.LOG_CONFLICT, 0);
        }
        if (index >= 0 && next < submitted[index]) {
            return reply(producer, next, AdmissionStatus.EXPIRED, 0);
        }
        if (index >= 0 && next == submitted[index]) {
            hash(input, offset, length);
            for (int i = 0; i < 32; i++) {
                if (hashes[index * 32 + i] != hash[i]) {
                    return reply(producer, next, AdmissionStatus.CONFLICT, 0);
                }
            }
            return reply(producer, next, AdmissionStatus.DUPLICATE,
                encodeEvent(input, offset, length, canonical[index], times[index], index));
        }
        if (index >= 0 && submitted[index] == Long.MAX_VALUE) {
            return reply(producer, next, AdmissionStatus.EXHAUSTED, 0);
        }
        if (next != (index < 0 ? 1 : submitted[index] + 1)) {
            return reply(producer, next, AdmissionStatus.GAP, 0);
        }
        if (index < 0 && count == limits.producers()) {
            return reply(producer, next, AdmissionStatus.CAPACITY, 0);
        }
        if (sequence == Long.MAX_VALUE) {
            return reply(producer, next, AdmissionStatus.EXHAUSTED, 0);
        }
        if (loggedTime < 0 || nanosPerTimeUnit <= 0 || loggedTime > Long.MAX_VALUE / nanosPerTimeUnit) {
            return reply(producer, next, AdmissionStatus.INVALID, 0);
        }
        final long timeNanos = loggedTime * nanosPerTimeUnit;
        hash(input, offset, length);
        final int target = index < 0 ? count : index;
        prefix.wrap(applicationResults, target * limits.resultBytes(), limits.resultBytes());
        if (index < 0) { prefix.setMemory(0, limits.resultBytes(), (byte)0); }
        final int prefixLength = application.apply(input, offset + Wire.PROPOSAL_OVERHEAD,
            length - Wire.PROPOSAL_OVERHEAD, proposal, timeNanos, prefix, 0);
        if (prefixLength > limits.resultBytes() || !Wire.validApplicationPrefix(prefix, 0, prefixLength)) {
            throw new IllegalStateException("application returned an invalid result prefix");
        }
        applicationLengths[target] = prefixLength;
        final int encoded = encodeEvent(input, offset, length, sequence + 1, timeNanos, target);
        if (index < 0) {
            producers.put(producer, target);
            ids[target] = producer;
            count++;
        }
        submitted[target] = next;
        canonical[target] = ++sequence;
        times[target] = timeNanos;
        System.arraycopy(hash, 0, hashes, target * 32, 32);
        eventLength = encoded;
        return reply(producer, next, AdmissionStatus.ACCEPTED, encoded);
    }

    private void hash(final DirectBuffer input, final int offset, final int length) {
        updateDigest(input, offset, length);
        finishDigest();
    }

    private void updateDigest(final DirectBuffer input, final int offset, final int length) {
        for (int position = 0; position < length;) {
            final int chunk = Math.min(ingress.length, length - position);
            input.getBytes(offset + position, ingress, 0, chunk);
            digest.update(ingress, 0, chunk);
            position += chunk;
        }
    }

    private void snapshotDigest(final DirectBuffer input, final int offset, final int length) {
        updateDigest(input, offset, 36);
        updateDigest(input, offset + 68, length - 68);
        finishDigest();
    }

    private void finishDigest() {
        try {
            digest.digest(hash, 0, hash.length);
        } catch (final DigestException ex) {
            throw new IllegalStateException("invalid fixed SHA-256 workspace", ex);
        }
    }

    private int encodeEvent(final DirectBuffer input, final int offset, final int length,
        final long cursor, final long timeNanos, final int producerIndex) {
        final ObservationProposalDecoder proposal = wire.proposal;
        final SequencedEventEncoder encoder = wire.eventEncoder.wrapAndApplyHeader(output,
            Wire.RESULT_OVERHEAD, wire.headerEncoder).logId(logId).sequence(cursor).sequencedTimeNanos(timeNanos)
            .sourceId(proposal.sourceId()).sourceConfigVersion(proposal.sourceConfigVersion()).clockId(proposal.clockId())
            .receiveTimeNanos(proposal.receiveTimeNanos()).completeTimeNanos(proposal.completeTimeNanos())
            .readyTimeNanos(proposal.readyTimeNanos());
        final int payloadOffset = offset + Wire.PROPOSAL_OVERHEAD, payloadLength = length - Wire.PROPOSAL_OVERHEAD;
        if (applicationLengths[producerIndex] == 0) {
            encoder.putPayload(input, payloadOffset, payloadLength);
        } else {
            final int destination = Wire.RESULT_OVERHEAD + Wire.EVENT_OVERHEAD;
            final int prefixLength = applicationLengths[producerIndex];
            output.putBytes(destination, applicationResults, producerIndex * limits.resultBytes(), prefixLength);
            output.putInt(destination + prefixLength, payloadLength, Wire.ORDER);
            output.putBytes(destination + prefixLength + 4, input, payloadOffset, payloadLength);
            encoder.putPayload(output, destination, prefixLength + 4 + payloadLength);
        }
        return Wire.HEADER_LENGTH + encoder.encodedLength();
    }

    private AdmissionStatus reply(final long producer, final long producerSequence,
        final AdmissionStatus status, final int encodedEventLength) {
        wire.resultEncoder.wrapAndApplyHeader(output, 0, wire.headerEncoder)
            .producerId(producer).producerSequence(producerSequence).status(status)
            .putCanonicalEvent(output, Wire.RESULT_OVERHEAD, encodedEventLength);
        resultLength = Wire.RESULT_OVERHEAD + encodedEventLength;
        return status;
    }

    public DirectBuffer resultBuffer() { return output; }
    public int resultLength() { return resultLength; }
    public int eventOffset() { return Wire.RESULT_OVERHEAD; }
    public int eventLength() { return eventLength; }
    public long logId() { return logId; }
    public long sequence() { return sequence; }
    public ApplicationLimits limits() { return limits; }
    /** Borrowed owner; access only on the core thread. Restore replaces it with the validated candidate. */
    public Application application() { return application; }

    /** Canonical unsigned producer order makes snapshots independent of map/insertion order. */
    public int snapshot() {
        eventLength = 0;
        for (int i = 0; i < count; i++) {
            int position = i;
            while (position > 0 && Long.compareUnsigned(ids[order[position - 1]], ids[i]) > 0) {
                order[position] = order[position - 1];
                position--;
            }
            order[position] = i;
        }
        final SequencerOutputSnapshotEncoder encoder = wire.snapshotEncoder
            .wrapAndApplyHeader(output, 0, wire.headerEncoder).logId(logId).sequence(sequence)
            .applicationId(Integer.toUnsignedLong(identity.id())).applicationVersion(identity.version())
            .configurationId(Integer.toUnsignedLong(identity.configurationId()));
        final SequencerOutputSnapshotEncoder.ReceiptsEncoder receipts = encoder.receiptsCount(count);
        for (int i = 0; i < count; i++) {
            final int index = order[i];
            receipts.next().producerId(ids[index]).producerSequence(submitted[index])
                .canonicalSequence(canonical[index]).sequencedTimeNanos(times[index])
                .putDigest(hashes, index * 32, 32)
                .putApplicationResult(applicationResults, index * limits.resultBytes(), applicationLengths[index]);
        }
        final int applicationOffset = Wire.HEADER_LENGTH + encoder.encodedLength() + 4;
        final int applicationLength = application.snapshot(output, applicationOffset);
        if (applicationLength < 0 || applicationLength > limits.stateBytes()) {
            throw new IllegalStateException("application snapshot exceeds its bound");
        }
        encoder.putApplicationState(output, applicationOffset, applicationLength);
        resultLength = Wire.HEADER_LENGTH + encoder.encodedLength();
        snapshotDigest(output, 0, resultLength);
        output.putBytes(36, hash);
        return resultLength;
    }

    /** Cold boundary: validate the entire snapshot before installing any state. */
    public void restore(final DirectBuffer input, final int offset, final int length) {
        if (!wire.validSnapshot(input, offset, length)) {
            throw new IllegalArgumentException("invalid snapshot layout");
        }
        if (!identity.matches(wire.snapshot.applicationId(), wire.snapshot.applicationVersion(),
            wire.snapshot.configurationId())) {
            throw new IllegalArgumentException("snapshot application identity mismatch");
        }
        snapshotDigest(input, offset, length);
        for (int i = 0; i < hash.length; i++) {
            if (input.getByte(offset + 36 + i) != hash[i]) {
                throw new IllegalArgumentException("snapshot digest mismatch");
            }
        }
        final long restoredLog = wire.snapshot.logId();
        final long restoredSequence = wire.snapshot.sequence();
        final int restoredCount = input.getShort(offset + 70, Wire.ORDER) & 65535;
        final int applicationOffset = wire.applicationStateOffset;
        if (restoredCount > limits.producers() || input.getInt(applicationOffset, Wire.ORDER) > limits.stateBytes()) {
            throw new IllegalArgumentException("snapshot exceeds application capacity declaration");
        }
        long sum = 0;
        long highest = 0;
        int row = offset + Wire.SNAPSHOT_OVERHEAD;
        for (int i = 0; i < restoredCount; i++) {
            order[i] = row;
            final long id = input.getLong(row, ByteOrder.LITTLE_ENDIAN);
            final long producerSequence = input.getLong(row + 8, ByteOrder.LITTLE_ENDIAN);
            final long cursor = input.getLong(row + 16, ByteOrder.LITTLE_ENDIAN);
            if (id == 0 || id == -1 || producerSequence <= 0 || producerSequence > cursor || cursor > restoredSequence
                || input.getLong(row + 24, ByteOrder.LITTLE_ENDIAN) < 0 || producerSequence > restoredSequence - sum) {
                throw new IllegalArgumentException("invalid snapshot receipt");
            }
            final int prefixLength = input.getInt(row + 64, Wire.ORDER);
            if (prefixLength > limits.resultBytes() || !Wire.validApplicationPrefix(input, row + 68, prefixLength)) {
                throw new IllegalArgumentException("invalid application receipt");
            }
            for (int j = 0; j < i; j++) {
                final int earlier = order[j];
                if (id == input.getLong(earlier, ByteOrder.LITTLE_ENDIAN)
                    || cursor == input.getLong(earlier + 16, ByteOrder.LITTLE_ENDIAN)) {
                    throw new IllegalArgumentException("duplicate snapshot receipt");
                }
            }
            sum += producerSequence;
            highest = Math.max(highest, cursor);
            row += Wire.RECEIPT_OVERHEAD + prefixLength;
        }
        if (sum != restoredSequence || highest != restoredSequence) {
            throw new IllegalArgumentException("snapshot omits admitted history");
        }
        final Application candidate = java.util.Objects.requireNonNull(application.restore(input, applicationOffset + 4,
            input.getInt(applicationOffset, Wire.ORDER), restoredSequence));
        for (int i = 0; i < restoredCount; i++) {
            candidate.validateResult(input, order[i] + 68, input.getInt(order[i] + 64, Wire.ORDER));
        }
        producers.clear();
        count = restoredCount;
        final SequencerOutputSnapshotDecoder.ReceiptsDecoder receipts = wire.snapshot.receipts();
        for (int i = 0; i < count; i++) {
            receipts.next();
            ids[i] = receipts.producerId();
            submitted[i] = receipts.producerSequence();
            canonical[i] = receipts.canonicalSequence();
            times[i] = receipts.sequencedTimeNanos();
            receipts.getDigest(hashes, i * 32, 32);
            applicationLengths[i] = receipts.applicationResultLength();
            receipts.getApplicationResult(applicationResults, i * limits.resultBytes(), applicationLengths[i]);
            producers.put(ids[i], i);
        }
        logId = restoredLog;
        sequence = restoredSequence;
        application = candidate;
        resultLength = 0;
        eventLength = 0;
    }
}
