package io.github.jstadeusiak.spine.protocol;

import io.github.jstadeusiak.spine.protocol.sbe.*;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

/** Single-owner reusable flyweights. Validation borrows input and rejects malformed messages without exceptions. */
public final class Wire {
    public static final int HEADER_LENGTH = 8, EVENT_OVERHEAD = 72, PROPOSAL_OVERHEAD = 72, RESULT_OVERHEAD = 29;
    public static final int INITIALIZE_LENGTH = 28, SNAPSHOT_OVERHEAD = 72, RECEIPT_OVERHEAD = 68, HEAD_LENGTH = 64, MAX_PRODUCERS = 1024;
    public static final int MAX_APPLICATION_PREFIX_BYTES = 2 * 1024 * 1024, APPLICATION_RESULT_OVERHEAD = MAX_APPLICATION_PREFIX_BYTES + 4;
    public static final int MAX_PAYLOAD_BYTES = 65536, MAX_EVENT_PAYLOAD_BYTES = MAX_PAYLOAD_BYTES + APPLICATION_RESULT_OVERHEAD;
    public static final int MAX_EVENT_BYTES = EVENT_OVERHEAD + MAX_EVENT_PAYLOAD_BYTES;
    public static final int MAX_PROPOSAL_BYTES = PROPOSAL_OVERHEAD + MAX_PAYLOAD_BYTES;
    public static final int MAX_RESULT_BYTES = RESULT_OVERHEAD + MAX_EVENT_BYTES;
    public static final int MAX_APPLICATION_STATE_BYTES = 64 * 1024 * 1024;
    public static final int MAX_SNAPSHOT_BYTES = 128 * 1024 * 1024;
    public static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    public final MessageHeaderDecoder header = new MessageHeaderDecoder();
    public final ObservationProposalDecoder proposal = new ObservationProposalDecoder();
    public final SequencedEventDecoder event = new SequencedEventDecoder();
    public final AdmissionResultDecoder result = new AdmissionResultDecoder();
    public final InitializeLogDecoder initialize = new InitializeLogDecoder();
    public final SequencerOutputSnapshotDecoder snapshot = new SequencerOutputSnapshotDecoder();
    public final RecordedHeadDecoder head = new RecordedHeadDecoder();
    public final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    public final ObservationProposalEncoder proposalEncoder = new ObservationProposalEncoder();
    public final SequencedEventEncoder eventEncoder = new SequencedEventEncoder();
    public final AdmissionResultEncoder resultEncoder = new AdmissionResultEncoder();
    public final InitializeLogEncoder initializeEncoder = new InitializeLogEncoder();
    public final SequencerOutputSnapshotEncoder snapshotEncoder = new SequencerOutputSnapshotEncoder();
    public final RecordedHeadEncoder headEncoder = new RecordedHeadEncoder();
    /** Absolute offset of the application-state length after a successful snapshot validation. */
    public int applicationStateOffset;

    public int templateId(final DirectBuffer buffer, final int offset, final int length) {
        if (!range(buffer, offset, length) || length < HEADER_LENGTH || length > MAX_SNAPSHOT_BYTES) return -1;
        header.wrap(buffer, offset);
        return header.schemaId() == 101 && header.version() == 1 ? header.templateId() : -1;
    }

    public boolean validInitialize(final DirectBuffer buffer, final int offset, final int length) {
        if (!fixed(buffer, offset, length, 2, 20, INITIALIZE_LENGTH)) return false;
        initialize.wrap(buffer, offset + HEADER_LENGTH, 20, header.version());
        return identity(initialize.logId()) && applicationIdentity(initialize.applicationId(),
            initialize.applicationVersion(), initialize.configurationId());
    }

    public boolean validProposal(final DirectBuffer buffer, final int offset, final int length) {
        if (!data(buffer, offset, length, 3, 60, PROPOSAL_OVERHEAD, MAX_PAYLOAD_BYTES)
            || !validInner(buffer, offset + PROPOSAL_OVERHEAD, length - PROPOSAL_OVERHEAD)) return false;
        proposal.wrap(buffer, offset + HEADER_LENGTH, 60, header.version());
        return identity(proposal.logId()) && identity(proposal.producerId()) && proposal.producerSequence() > 0
            && metadata(proposal.sourceId(), proposal.sourceConfigVersion(), proposal.clockId(),
                proposal.receiveTimeNanos(), proposal.completeTimeNanos(), proposal.readyTimeNanos());
    }

    public boolean validEvent(final DirectBuffer buffer, final int offset, final int length) {
        if (!data(buffer, offset, length, 1, 60, EVENT_OVERHEAD, MAX_EVENT_PAYLOAD_BYTES)
            || !validInner(buffer, offset + EVENT_OVERHEAD, length - EVENT_OVERHEAD)) return false;
        event.wrap(buffer, offset + HEADER_LENGTH, 60, header.version());
        return identity(event.logId()) && event.sequence() > 0 && event.sequencedTimeNanos() >= -1
            && metadata(event.sourceId(), event.sourceConfigVersion(), event.clockId(),
                event.receiveTimeNanos(), event.completeTimeNanos(), event.readyTimeNanos());
    }

    public boolean validResult(final DirectBuffer buffer, final int offset, final int length) {
        if (!data(buffer, offset, length, 4, 17, RESULT_OVERHEAD, MAX_EVENT_BYTES)) return false;
        final int status = buffer.getByte(offset + 24) & 255;
        if (status > 10) return false;
        result.wrap(buffer, offset + HEADER_LENGTH, 17, header.version());
        final boolean correlated = identity(result.producerId()) && result.producerSequence() > 0;
        final boolean empty = result.producerId() == 0 && result.producerSequence() == 0;
        return status == 1 || status == 2
            ? correlated && validEvent(buffer, offset + RESULT_OVERHEAD, length - RESULT_OVERHEAD)
            : (correlated || empty) && length == RESULT_OVERHEAD;
    }

    public boolean validSnapshot(final DirectBuffer buffer, final int offset, final int length) {
        if (templateId(buffer, offset, length) != 6 || header.blockLength() != 60 || length < SNAPSHOT_OVERHEAD) return false;
        final int count = buffer.getShort(offset + 70, ORDER) & 65535;
        if ((buffer.getShort(offset + 68, ORDER) & 65535) != RECEIPT_OVERHEAD - 4 || count > MAX_PRODUCERS) return false;
        int state = SNAPSHOT_OVERHEAD;
        for (int i = 0; i < count; i++) {
            if (state > length - RECEIPT_OVERHEAD) return false;
            final int resultLength = buffer.getInt(offset + state + RECEIPT_OVERHEAD - 4, ORDER);
            if (resultLength < 0 || resultLength > MAX_APPLICATION_PREFIX_BYTES
                || resultLength > length - state - RECEIPT_OVERHEAD) return false;
            state += RECEIPT_OVERHEAD + resultLength;
        }
        if (state > length - 4 || buffer.getInt(offset + state, ORDER) != length - state - 4
            || length - state - 4 > MAX_APPLICATION_STATE_BYTES) return false;
        snapshot.wrap(buffer, offset + HEADER_LENGTH, 60, header.version());
        if (!applicationIdentity(snapshot.applicationId(), snapshot.applicationVersion(), snapshot.configurationId())
            || snapshot.logId() == -1 || snapshot.sequence() < 0
            || snapshot.logId() == 0 && (snapshot.sequence() != 0 || count != 0)) return false;
        applicationStateOffset = offset + state;
        return true;
    }

    public boolean validHead(final DirectBuffer buffer, final int offset, final int length) {
        if (!fixed(buffer, offset, length, 7, 56, HEAD_LENGTH)) return false;
        head.wrap(buffer, offset + HEADER_LENGTH, 56, header.version());
        return identity(head.logId()) && head.sequence() >= 0 && head.archiveId() >= 0 && head.recordingId() >= 0
            && head.position() >= 0 && head.beaconSequence() > 0;
    }

    public int encodeInitialize(final MutableDirectBuffer dst, final int offset, final long logId) {
        return encodeInitialize(dst, offset, logId, ApplicationIdentity.PASS_THROUGH);
    }

    public int encodeInitialize(final MutableDirectBuffer dst, final int offset, final long logId,
        final ApplicationIdentity identity) {
        require(identity(logId) && range(dst, offset, INITIALIZE_LENGTH));
        initializeEncoder.wrapAndApplyHeader(dst, offset, headerEncoder).logId(logId)
            .applicationId(Integer.toUnsignedLong(identity.id())).applicationVersion(identity.version())
            .configurationId(Integer.toUnsignedLong(identity.configurationId()));
        return INITIALIZE_LENGTH;
    }

    public int encodeProposal(final MutableDirectBuffer dst, final int offset, final long logId,
        final long producerId, final long sequence, final int source, final int config, final int clock,
        final long receive, final long complete, final long ready,
        final DirectBuffer payload, final int payloadOffset, final int payloadLength) {
        require(identity(logId) && identity(producerId) && sequence > 0
            && metadata(Integer.toUnsignedLong(source), Integer.toUnsignedLong(config), Integer.toUnsignedLong(clock),
                receive, complete, ready) && payloadLength <= MAX_PAYLOAD_BYTES
            && validInner(payload, payloadOffset, payloadLength) && range(dst, offset, PROPOSAL_OVERHEAD + payloadLength));
        proposalEncoder.wrapAndApplyHeader(dst, offset, headerEncoder).logId(logId).producerId(producerId)
            .producerSequence(sequence).sourceId(Integer.toUnsignedLong(source)).sourceConfigVersion(Integer.toUnsignedLong(config))
            .clockId(Integer.toUnsignedLong(clock)).receiveTimeNanos(receive).completeTimeNanos(complete).readyTimeNanos(ready)
            .putPayload(payload, payloadOffset, payloadLength);
        return PROPOSAL_OVERHEAD + payloadLength;
    }

    public int encodeEvent(final MutableDirectBuffer dst, final int offset, final long logId, final long sequence,
        final long time, final int source, final int config, final int clock, final long receive,
        final long complete, final long ready, final DirectBuffer payload, final int payloadOffset, final int payloadLength) {
        require(identity(logId) && sequence > 0 && time >= -1
            && metadata(Integer.toUnsignedLong(source), Integer.toUnsignedLong(config), Integer.toUnsignedLong(clock),
                receive, complete, ready) && payloadLength <= MAX_EVENT_PAYLOAD_BYTES
            && validInner(payload, payloadOffset, payloadLength) && range(dst, offset, EVENT_OVERHEAD + payloadLength));
        eventEncoder.wrapAndApplyHeader(dst, offset, headerEncoder).logId(logId).sequence(sequence).sequencedTimeNanos(time)
            .sourceId(Integer.toUnsignedLong(source)).sourceConfigVersion(Integer.toUnsignedLong(config))
            .clockId(Integer.toUnsignedLong(clock)).receiveTimeNanos(receive).completeTimeNanos(complete).readyTimeNanos(ready)
            .putPayload(payload, payloadOffset, payloadLength);
        return EVENT_OVERHEAD + payloadLength;
    }

    public int encodeResult(final MutableDirectBuffer dst, final int offset, final long producerId, final long sequence,
        final AdmissionStatus status, final DirectBuffer canonical, final int eventOffset, final int eventLength) {
        final boolean hasEvent = status == AdmissionStatus.ACCEPTED || status == AdmissionStatus.DUPLICATE;
        require(status != null && status != AdmissionStatus.NULL_VAL
            && (identity(producerId) && sequence > 0 || !hasEvent && producerId == 0 && sequence == 0)
            && (hasEvent ? validEvent(canonical, eventOffset, eventLength) : eventLength == 0)
            && range(dst, offset, RESULT_OVERHEAD + eventLength));
        resultEncoder.wrapAndApplyHeader(dst, offset, headerEncoder).producerId(producerId).producerSequence(sequence).status(status);
        // Writing an empty field uses the owned destination, avoiding an allocated empty array.
        resultEncoder.putCanonicalEvent(hasEvent ? canonical : dst, hasEvent ? eventOffset : offset, eventLength);
        return RESULT_OVERHEAD + eventLength;
    }

    public int encodeHead(final MutableDirectBuffer dst, final int offset, final long logId, final long sequence,
        final long archiveId, final int streamId, final long recordingId, final int sessionId,
        final long position, final long beaconSequence) {
        require(identity(logId) && sequence >= 0 && archiveId >= 0 && recordingId >= 0 && position >= 0
            && beaconSequence > 0 && range(dst, offset, HEAD_LENGTH));
        headEncoder.wrapAndApplyHeader(dst, offset, headerEncoder).logId(logId).sequence(sequence).archiveId(archiveId)
            .outputStreamId(streamId).recordingId(recordingId).sessionId(sessionId).position(position).beaconSequence(beaconSequence);
        return HEAD_LENGTH;
    }

    /** An application result may include SBE groups/data before Spine's exact original-input varData. */
    public static boolean validApplicationPrefix(final DirectBuffer buffer, final int offset, final int length) {
        return length == 0 || length <= MAX_APPLICATION_PREFIX_BYTES && validInner(buffer, offset, length);
    }

    public static boolean validInner(final DirectBuffer buffer, final int offset, final int length) {
        return range(buffer, offset, length) && length >= HEADER_LENGTH
            && (buffer.getShort(offset, ORDER) & 65535) <= length - HEADER_LENGTH;
    }

    public static boolean range(final DirectBuffer buffer, final int offset, final int length) {
        return buffer != null && offset >= 0 && length >= 0 && offset <= buffer.capacity() && length <= buffer.capacity() - offset;
    }

    public static boolean identity(final long value) { return value != 0 && value != -1; }

    private boolean fixed(final DirectBuffer buffer, final int offset, final int length,
        final int template, final int block, final int expected) {
        return templateId(buffer, offset, length) == template && header.blockLength() == block && length == expected;
    }

    private boolean data(final DirectBuffer buffer, final int offset, final int length, final int template,
        final int block, final int overhead, final int maximum) {
        return templateId(buffer, offset, length) == template && header.blockLength() == block && length >= overhead
            && length - overhead <= maximum && buffer.getInt(offset + overhead - 4, ORDER) == length - overhead;
    }

    private static boolean applicationIdentity(final long id, final long version, final long configuration) {
        return id > 0 && id < 0xFFFF_FFFFL && version > 0 && version <= Integer.MAX_VALUE
            && configuration > 0 && configuration < 0xFFFF_FFFFL;
    }

    private static boolean metadata(final long source, final long config, final long clock,
        final long receive, final long complete, final long ready) {
        return source != 0xFFFF_FFFFL && config != 0xFFFF_FFFFL && clock != 0xFFFF_FFFFL
            && receive >= -1 && complete >= -1 && ready >= -1;
    }

    static void require(final boolean condition) {
        if (!condition) throw new IllegalArgumentException("invalid wire encoding arguments");
    }
}
