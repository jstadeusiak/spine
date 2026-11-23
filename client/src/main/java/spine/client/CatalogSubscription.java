package spine.client;

import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import spine.protocol.Wire;

/**
 * Independent recorded-history consumer. One selected Archive replay follows the recording as it grows.
 * Startup/requalification verifies every overlapping retained source, then delivery uses fixed owned buffers.
 * A stopped generation requires explicit refresh(), a cold administrative operation; idle polling never rescans.
 * The supplied Archive/Aeron are borrowed. Checkpoints, application atomicity and retention remain application-owned.
 */
public final class CatalogSubscription implements AutoCloseable {
    private final AeronArchive archive;
    private final long logId;
    private final int streamId;
    private final String replayChannel;
    private final int replayStreamId;
    private final ContiguousEventDispatcher dispatcher;
    private final Wire wire = new Wire();
    private final FragmentHandler messages = this::onMessage;
    private RecordingCatalog catalog;
    private RecordingCatalog.Span span;
    private BoundedReplay replay;
    private EventHandler handler;
    private long sourceSequence;
    private boolean selectedStopped;
    private SubscriptionStatus status = SubscriptionStatus.CONNECTING;
    private Throwable failure;

    /**
     * Start after the sequence stored with the application's checkpoint. Use zero for an empty projection.
     * Construction scans retained recordings and can fail if they lack a contiguous suffix for this log.
     */
    public CatalogSubscription(final AeronArchive archive, final long logId, final int streamId,
                               final long lastApplied, final String replayChannel, final int replayStreamId) {
        this.archive = archive;
        this.logId = logId;
        this.streamId = streamId;
        this.replayChannel = replayChannel;
        this.replayStreamId = replayStreamId;
        dispatcher = new ContiguousEventDispatcher(logId, lastApplied);
        refresh();
    }

    /** Cold qualification of complete recorded coverage and all overlaps after the current application cursor. */
    public void refresh() {
        if (status == SubscriptionStatus.CLOSED || status == SubscriptionStatus.FAILED) {
            throw new IllegalStateException("subscription stopped", failure);
        }
        try {
            if (replay != null) {
                replay.close();
                replay = null;
            }
            catalog = RecordingCatalog.rebuild(archive, logId, streamId, sequence(), replayChannel, replayStreamId);
            selectedStopped = false;
            openNext();
        } catch (final RuntimeException | Error error) {
            stop(error);
            throw error;
        }
    }

    /**
     * Poll at most {@code fragmentLimit} transport fragments, returning the fragment count. A complete
     * event advances {@link #sequence()} after the handler returns. A handler failure stops this subscription.
     */
    public int poll(final EventHandler handler, final int fragmentLimit) {
        if (handler == null || fragmentLimit <= 0) { throw new IllegalArgumentException("handler and fragment limit required"); }
        if (status == SubscriptionStatus.CLOSED || status == SubscriptionStatus.FAILED) {
            throw new IllegalStateException("subscription stopped", failure);
        }
        this.handler = handler;
        if (replay == null) { return 0; }
        try {
            final int work = replay.poll(messages, fragmentLimit);
            if (span.active() && sourceSequence != 0 && sourceSequence >= span.lastSequence() && !replay.unfinished()) {
                status = SubscriptionStatus.LIVE;
            }
            if (replay.done()) {
                if (replay.unfinished()) { throw new IllegalStateException("recording ended inside a message"); }
                replay.close();
                replay = null;
                if (span.active()) {
                    selectedStopped = true;
                    status = SubscriptionStatus.REPLAYING;
                } else {
                    openNext();
                }
            }
            return work;
        } catch (final RuntimeException | Error error) {
            stop(error);
            throw error;
        }
    }

    private void onMessage(final org.agrona.DirectBuffer buffer, final int offset, final int length,
                           final io.aeron.logbuffer.Header header) {
        if (!wire.validEvent(buffer, offset, length) || wire.event.logId() != logId
            || (sourceSequence != 0 && (sourceSequence == Long.MAX_VALUE || wire.event.sequence() != sourceSequence + 1))) {
            throw new IllegalStateException("selected source is malformed or noncontiguous");
        }
        sourceSequence = wire.event.sequence();
        dispatcher.onMessage(buffer, offset, length, handler, sourceSequence <= span.lastSequence());
    }

    private void openNext() {
        status = SubscriptionStatus.REPLAYING;
        if (sequence() == Long.MAX_VALUE || selectedStopped) { return; }
        span = catalog.select(sequence() + 1);
        if (span != null) {
            sourceSequence = 0;
            replay = new BoundedReplay(archive, span.recordingId(), span.startPosition(), span.completePosition(),
                replayChannel, replayStreamId, span.active());
        }
    }

    private void stop(final Throwable error) {
        failure = error;
        status = SubscriptionStatus.FAILED;
        if (replay != null) {
            try { replay.close(); }
            catch (final RuntimeException | Error closeError) { error.addSuppressed(closeError); }
            replay = null;
        }
    }

    public long sequence() { return dispatcher.sequence(); }
    public SubscriptionStatus status() { return status; }
    public Throwable failureReason() { return failure; }
    public boolean requiresRefresh() { return selectedStopped || replay == null; }
    public long activeRecordingId() { return replay == null ? -1 : span.recordingId(); }

    @Override public void close() {
        status = SubscriptionStatus.CLOSED;
        if (replay != null) {
            replay.close();
            replay = null;
        }
    }
}
