package io.github.jstadeusiak.spine.feeds;

import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.client.ReliableProducer;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/** One owner: exact capture before admission. Archive is borrowed; publication and producer are owned. */
public final class CapturedIngress implements AutoCloseable {
    private static final String IPC = "aeron:ipc?term-length=1m";
    private final ReliableProducer producer;
    private final int sourceId;
    private final CountersReader counters;
    private final int counter;
    private final long recordingId;
    private final long registration;
    private ExclusivePublication capture;
    private long position;
    private long configuration;
    private long clockId;
    private boolean recorded;
    private boolean closed;
    private Throwable failure;

    /** Cold recovery invokes a borrowed, read-only callback for each complete proposal, with a null Header. */
    public CapturedIngress(final AeronArchive archive, final AeronCluster.Context context, final long logId,
        final long producerId, final int sourceId, final int captureStream, final int replayStream,
        final FragmentHandler recovered) {
        if (!Wire.identity(logId) || !Wire.identity(producerId) || sourceId == -1) {
            throw new IllegalArgumentException("provisioned log, producer and source required");
        }
        this.sourceId = sourceId;
        final String alias = "spine-capture-" + Long.toUnsignedString(producerId);
        final long timeout = archive.context().messageTimeoutNs();
        final Recovery state = new Recovery(logId, producerId, sourceId, recovered);
        recover(archive, alias, captureStream, replayStream, timeout, state);
        producer = new ReliableProducer(logId, producerId, state.sequence == 0 ? 1 : state.sequence);
        configuration = state.configuration;
        clockId = state.clockId;
        counters = archive.context().aeron().countersReader();
        try {
            if (state.length != 0) {
                if (!producer.restoreExact(state.latest, 0, state.length)) {
                    throw new IllegalStateException("captured proposal restore refused");
                }
                recorded = true;
            }
            producer.connect(context);
            capture = archive.addRecordedExclusivePublication(IPC + "|alias=" + alias, captureStream);
            final long deadline = System.nanoTime() + timeout;
            int found;
            long registered;
            while ((found = RecordingPos.findCounterIdBySession(counters, capture.sessionId(), archive.archiveId()))
                == CountersReader.NULL_COUNTER_ID
                || (registered = counters.getCounterRegistrationId(found)) == CountersReader.DEFAULT_REGISTRATION_ID) {
                producer.poll(false);
                if (System.nanoTime() - deadline >= 0) { throw new IllegalStateException("capture recorder absent"); }
                LockSupport.parkNanos(100_000);
            }
            counter = found;
            recordingId = RecordingPos.getRecordingId(counters, counter);
            registration = registered;
            requireRecorder();
            if (RecordingPos.findCounterIdBySession(counters, capture.sessionId(), archive.archiveId()) != counter) {
                throw new IllegalStateException("capture recording changed during setup");
            }
            requireRecorder();
        } catch (final RuntimeException | Error error) {
            CloseHelper.quietCloseAll(capture, producer);
            throw error;
        }
    }

    public boolean idle() { return !closed && failure == null && producer.pendingLength() == 0; }

    public boolean submit(final int configuration, final int clock, final long receive, final long complete,
        final long ready, final DirectBuffer payload, final int offset, final int length) {
        checkRunning();
        if (!producer.submit(sourceId, configuration, clock, receive, complete, ready, payload, offset, length)) { return false; }
        this.configuration = Integer.toUnsignedLong(configuration);
        clockId = Integer.toUnsignedLong(clock);
        position = 0;
        recorded = false;
        return true;
    }

    public int poll() {
        checkRunning();
        try {
            requireRecorder();
            int work = 0;
            if (producer.pendingLength() != 0 && !recorded) {
                if (position == 0) {
                    final long offered = capture.offer(producer.pendingBuffer(), 0, producer.pendingLength());
                    if (offered > 0) { position = offered; work++; }
                    else if (offered != Publication.BACK_PRESSURED && offered != Publication.ADMIN_ACTION
                        && offered != Publication.NOT_CONNECTED) { throw new IllegalStateException("capture publication unavailable"); }
                }
                if (position != 0) {
                    final long complete = counters.getCounterValue(counter);
                    requireRecorder();
                    recorded = complete >= position;
                }
            }
            work += producer.poll(recorded);
            if (producer.pendingLength() != 0 && producer.lastStatus() != null) {
                throw new IllegalStateException("captured proposal rejected: " + producer.lastStatus());
            }
            if (producer.nextSequence() == 0) { throw new IllegalStateException("producer sequence exhausted"); }
            return work;
        } catch (final RuntimeException | Error error) { failure = error; throw error; }
    }

    public long lastConfiguration() { return configuration; }
    public long lastClockId() { return clockId; }
    public long acknowledgedSequence() { return producer.acknowledgedSequence(); }

    private void requireRecorder() {
        if (counters.getCounterRegistrationId(counter) != registration
            || !RecordingPos.isActive(counters, counter, recordingId)) {
            throw new IllegalStateException("capture recording stopped or counter reused");
        }
    }

    private void checkRunning() {
        if (closed || failure != null) { throw new IllegalStateException("captured ingress stopped", failure); }
    }

    @Override public void close() { closed = true; CloseHelper.closeAll(capture, producer); }

    private record Recording(long id, long startPosition, long stopPosition) {}

    private static void recover(final AeronArchive archive, final String alias, final int stream, final int replayStream,
        final long timeout, final Recovery state) {
        final List<Recording> recordings = new ArrayList<>();
        final long[] next = {0};
        int count;
        do {
            count = archive.listRecordings(next[0], 100, (control, correlation, id, startTime, stopTime, start, stop,
                termId, segment, term, mtu, session, recordedStream, stripped, original, identity) -> {
                next[0] = id + 1;
                if (recordedStream == stream && alias.equals(ChannelUri.parse(original).get("alias"))) {
                    recordings.add(new Recording(id, start, stop));
                }
            });
        } while (count == 100);
        for (int i = 0; i < recordings.size(); i++) {
            final Recording recording = recordings.get(i);
            long end = recording.stopPosition();
            final long deadline = System.nanoTime() + timeout;
            while (end == AeronArchive.NULL_POSITION) {
                end = archive.getStopPosition(recording.id());
                if (System.nanoTime() - deadline >= 0) { throw new IllegalStateException("previous capture owner still active"); }
                LockSupport.parkNanos(100_000);
            }
            if (end < recording.startPosition() || ((end | recording.startPosition()) & 31) != 0) {
                throw new IllegalStateException("invalid capture recording fence");
            }
            state.completePosition = recording.startPosition();
            if (end != recording.startPosition()) {
                replay(archive, recording.id(), recording.startPosition(), end, replayStream, timeout, state);
            }
            if (state.assembling) {
                if (i != recordings.size() - 1) { throw new IllegalStateException("incomplete prior capture generation"); }
                // This tail was never offered under capture-before-admission. Remove it before a new generation
                // makes it an earlier discontinuity on the next restart; retain every complete proposal.
                archive.truncateRecording(recording.id(), state.completePosition);
            }
        }
    }

    private static void replay(final AeronArchive archive, final long recording, final long start, final long end,
        final int stream, final long timeout, final Recovery state) {
        try (Subscription subscription = archive.context().aeron().addSubscription(IPC, stream)) {
            final long replay = archive.startReplay(recording, start, end - start, IPC, stream);
            try {
                long deadline = System.nanoTime() + timeout;
                Image image = null;
                while (image == null || image.position() < end) {
                    if (image == null) { image = subscription.imageBySessionId((int)replay); }
                    final int work = image == null ? 0 : image.poll(state, 256);
                    if (state.failure != null) { LangUtil.rethrowUnchecked(state.failure); }
                    if (work > 0) { deadline = System.nanoTime() + timeout; }
                    if (image != null && image.position() >= end) { break; }
                    if ((image != null && image.isClosed()) || System.nanoTime() - deadline >= 0) {
                        throw new IllegalStateException("capture replay stopped before fence");
                    }
                    if (work == 0) { LockSupport.parkNanos(100_000); }
                }
            } finally {
                try { archive.stopReplay(replay); }
                catch (final ArchiveException error) {
                    if (error.errorCode() != ArchiveException.UNKNOWN_REPLAY) { throw error; }
                }
            }
        }
    }

    private static final class Recovery implements FragmentHandler {
        final UnsafeBuffer latest = new UnsafeBuffer(new byte[Wire.MAX_PROPOSAL_BYTES]);
        final Wire wire = new Wire();
        final ImageFragmentAssembler assembler;
        long sequence, configuration, clockId, completePosition;
        int length, messageBytes, nextOffset, termId;
        boolean assembling;
        Throwable failure;

        Recovery(final long logId, final long producerId, final int sourceId, final FragmentHandler recovered) {
            assembler = new ImageFragmentAssembler((buffer, offset, size, header) -> {
                if (!wire.validProposal(buffer, offset, size) || wire.proposal.logId() != logId
                    || wire.proposal.producerId() != producerId || wire.proposal.sourceId() != Integer.toUnsignedLong(sourceId)
                    || sequence == Long.MAX_VALUE || wire.proposal.producerSequence() != sequence + 1) {
                    throw new IllegalStateException("invalid or noncontiguous capture proposal");
                }
                latest.putBytes(0, buffer, offset, size);
                length = size;
                sequence = wire.proposal.producerSequence();
                configuration = wire.proposal.sourceConfigVersion();
                clockId = wire.proposal.clockId();
                if (recovered != null) { recovered.onFragment(buffer, offset, size, null); }
            }, Wire.MAX_PROPOSAL_BYTES, false);
        }

        @Override public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (failure != null) { return; }
            try {
                final byte flags = header.flags();
                if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED) {
                    if (assembling) { throw new IllegalStateException("unfinished captured message"); }
                    messageBytes = length;
                } else if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) != 0) {
                    if (assembling) { throw new IllegalStateException("nested capture fragment"); }
                    assembling = true;
                    termId = header.termId();
                    messageBytes = length;
                } else {
                    if (!assembling || header.termId() != termId || header.termOffset() != nextOffset) {
                        throw new IllegalStateException("discontinuous capture fragment");
                    }
                    messageBytes += length;
                }
                if (messageBytes > Wire.MAX_PROPOSAL_BYTES) { throw new IllegalStateException("oversized capture proposal"); }
                nextOffset = header.nextTermOffset();
                assembler.onFragment(buffer, offset, length, header);
                if ((flags & FrameDescriptor.END_FRAG_FLAG) != 0) {
                    assembling = false;
                    completePosition = header.position();
                }
            } catch (final RuntimeException | Error error) { failure = error; }
        }
    }
}
