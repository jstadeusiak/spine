package spine.client;

import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Single owner, one pending proposal, exact retries across reconnects. This memory is not a restart checkpoint:
 * the caller must fence complete recording of the exact proposal in its input Archive before offering if loss is forbidden.
 * Restart requires the latest complete captured proposal and its producer sequence. A receipt is admission, not output recording.
 */
public final class ReliableProducer implements AutoCloseable {
    public static final long RETRY_INTERVAL_NS = 10_000_000L;
    private final long logId;
    private final long producerId;
    private final Wire wire = new Wire();
    private final UnsafeBuffer pending = new UnsafeBuffer(new byte[Wire.MAX_PROPOSAL_BYTES]);
    private long nextSequence;
    private long acknowledgedSequence;
    private int pendingLength;
    private AdmissionStatus lastStatus;
    private AeronCluster client;
    private long nextOffer;
    private long lastKeepAlive;
    private Throwable failure;
    private boolean closed;

    public ReliableProducer(final long logId, final long producerId, final long nextSequence) {
        if (logId == 0 || logId == -1 || producerId == 0 || producerId == -1 || nextSequence < 1) {
            throw new IllegalArgumentException("provisioned identities and positive producer sequence required");
        }
        this.logId = logId;
        this.producerId = producerId;
        this.nextSequence = nextSequence;
    }

    /** A full slot or exhausted sequence refuses another observation without mutation. */
    public boolean submit(final int sourceId, final int config, final int clock, final long receive,
                          final long complete, final long ready, final DirectBuffer payload,
                          final int offset, final int length) {
        checkFailure();
        if (pendingLength != 0 || nextSequence == 0 || sourceId == -1 || config == -1 || clock == -1
            || receive < -1 || complete < -1 || ready < -1 || offset < 0 || length < 8
            || length > Wire.MAX_PAYLOAD_BYTES || !Wire.validInner(payload, offset, length)) { return false; }
        final int encoded = wire.encodeProposal(pending, 0, logId, producerId, nextSequence,
            sourceId, config, clock, receive, complete, ready, payload, offset, length);
        if (!wire.validProposal(pending, 0, encoded)) { return false; }
        pendingLength = encoded;
        lastStatus = null;
        nextOffer = 0;
        return true;
    }

    /** Restore an exact recorded proposal before connecting. No regeneration of its times or payload is allowed. */
    public boolean restoreExact(final DirectBuffer bytes, final int offset, final int length) {
        checkFailure();
        if (pendingLength != 0 || !wire.validProposal(bytes, offset, length)
            || wire.proposal.logId() != logId || wire.proposal.producerId() != producerId
            || wire.proposal.producerSequence() != nextSequence) { return false; }
        pending.putBytes(0, bytes, offset, length);
        pendingLength = length;
        lastStatus = null;
        nextOffer = 0;
        return true;
    }

    /** Owns the fresh context and session. Context ownsAeronClient controls the underlying Aeron ownership. */
    public void connect(final AeronCluster.Context context) {
        checkFailure();
        if (client != null) { client.close(); }
        client = AeronCluster.connect(context.egressListener((session, timestamp, buffer, offset, length, header) ->
            onReceipt(buffer, offset, length)));
        nextOffer = 0;
        lastKeepAlive = System.nanoTime();
    }

    public int poll() { return poll(true); }

    /**
     * Poll egress and keepalives. The caller's complete-capture authority controls whether offers/retries are allowed.
     * False suppresses offers/retries without undoing prior admission or changing pending-slot/rejection semantics.
     */
    public int poll(final boolean offerPending) {
        checkFailure();
        if (client == null || client.isClosed()) { throw new IllegalStateException("connect a fresh Cluster session"); }
        int work = client.pollEgress();
        checkFailure();
        final long now = System.nanoTime();
        if (offerPending && pendingLength != 0 && lastStatus == null && (nextOffer == 0 || now - nextOffer >= 0)) {
            final long offered = client.offer(pending, 0, pendingLength);
            if (offered > 0) {
                nextOffer = now + RETRY_INTERVAL_NS;
                work++;
            } else if (offered == Publication.CLOSED || offered == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("Cluster ingress closed or exhausted; reconnect");
            }
        }
        if (now - lastKeepAlive >= 500_000_000L && client.sendKeepAlive()) {
            lastKeepAlive = now;
            work++;
        }
        return work;
    }

    /** Called by the owned egress listener. Ordinary correlated rejection leaves exact pending bytes available. */
    public void onReceipt(final DirectBuffer buffer, final int offset, final int length) {
        if (failure != null) { return; }
        try {
            if (!wire.validResult(buffer, offset, length)) { throw new IllegalStateException("malformed receipt"); }
            final long producerSequence = wire.result.producerSequence();
            if (wire.result.status() == AdmissionStatus.INVALID && wire.result.producerId() == 0
                && producerSequence == 0 && pendingLength != 0) {
                lastStatus = AdmissionStatus.INVALID;
                return;
            }
            if (wire.result.producerId() != producerId || nextSequence == 0 || producerSequence < nextSequence) { return; }
            if (pendingLength == 0 || producerSequence != nextSequence) {
                throw new IllegalStateException("receipt does not correlate with pending proposal");
            }
            final AdmissionStatus status = wire.result.status();
            if (status != AdmissionStatus.ACCEPTED && status != AdmissionStatus.DUPLICATE) {
                lastStatus = status;
                return;
            }
            wire.validProposal(pending, 0, pendingLength);
            final int eventOffset = offset + Wire.RESULT_OVERHEAD;
            final int eventLength = length - Wire.RESULT_OVERHEAD;
            if (!wire.validEvent(buffer, eventOffset, eventLength)
                || wire.event.logId() != logId || wire.event.sequence() <= acknowledgedSequence
                || wire.event.sequencedTimeNanos() < 0
                || wire.event.sourceId() != wire.proposal.sourceId()
                || wire.event.sourceConfigVersion() != wire.proposal.sourceConfigVersion()
                || wire.event.clockId() != wire.proposal.clockId()
                || wire.event.receiveTimeNanos() != wire.proposal.receiveTimeNanos()
                || wire.event.completeTimeNanos() != wire.proposal.completeTimeNanos()
                || wire.event.readyTimeNanos() != wire.proposal.readyTimeNanos()) {
                throw new IllegalStateException("receipt metadata differs");
            }
            int observationOffset = eventOffset + Wire.EVENT_OVERHEAD;
            if (eventLength != pendingLength) {
                final int prefixLength = eventLength - pendingLength - Integer.BYTES;
                if (prefixLength < Wire.HEADER_LENGTH || !Wire.validApplicationPrefix(buffer, observationOffset, prefixLength)
                    || buffer.getInt(observationOffset + prefixLength, Wire.ORDER)
                        != pendingLength - Wire.PROPOSAL_OVERHEAD) {
                    throw new IllegalStateException("receipt observation framing differs");
                }
                observationOffset += prefixLength + Integer.BYTES;
            }
            for (int i = 0; i < pendingLength - Wire.PROPOSAL_OVERHEAD; i++) {
                if (buffer.getByte(observationOffset + i) != pending.getByte(Wire.PROPOSAL_OVERHEAD + i)) {
                    throw new IllegalStateException("receipt payload differs");
                }
            }
            acknowledgedSequence = wire.event.sequence();
            nextSequence = nextSequence == Long.MAX_VALUE ? 0 : nextSequence + 1;
            pendingLength = 0;
            lastStatus = status;
        } catch (final RuntimeException | Error error) { failure = error; }
    }

    /** Resume this exact proposal after the caller resolves an ordinary rejection. */
    public boolean retryPending() {
        checkFailure();
        if (pendingLength == 0) { return false; }
        lastStatus = null;
        nextOffer = 0;
        return true;
    }

    public DirectBuffer pendingBuffer() { return pending; }
    public int pendingLength() { return pendingLength; }
    public long nextSequence() { return nextSequence; }
    public long acknowledgedSequence() { return acknowledgedSequence; }
    public AdmissionStatus lastStatus() { return lastStatus; }
    public Throwable failureReason() { return failure; }

    private void checkFailure() {
        if (closed || failure != null) { throw new IllegalStateException("producer stopped", failure); }
    }

    @Override
    public void close() {
        closed = true;
        if (client != null) {
            client.close();
        }
    }
}
