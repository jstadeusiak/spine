package io.github.jstadeusiak.spine.client;

import io.aeron.ChannelUri;
import io.aeron.archive.client.AeronArchive;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/** Cold, rebuildable output catalog. Overlaps are SHA-256 checked; only complete recorded frames form its head. */
public final class RecordingCatalog {
    public record Span(long recordingId, int sessionId, long startPosition, long completePosition,
                       long firstSequence, long lastSequence, boolean active) { }
    private final long logId;
    private final List<Span> spans;
    private final long head;

    private RecordingCatalog(final long logId, final List<Span> spans, final long head) {
        this.logId = logId;
        this.spans = List.copyOf(spans);
        this.head = head;
    }

    /** Qualify from genesis; use the checkpoint overload only with trusted application state at that cursor. */
    public static RecordingCatalog rebuild(final AeronArchive archive, final long logId, final int streamId,
                                           final String replayChannel, final int replayStreamId) {
        return rebuild(archive, logId, streamId, 0, replayChannel, replayStreamId);
    }

    /** Cold: scans retained history; hashes and verifies every retained overlap after the application checkpoint. */
    public static RecordingCatalog rebuild(final AeronArchive archive, final long logId, final int streamId,
        final long lastApplied, final String replayChannel, final int replayStreamId) {
        if (!Wire.identity(logId) || lastApplied < 0) { throw new IllegalArgumentException("invalid checkpoint"); }
        final List<Span> candidates = new ArrayList<>();
        final long[] next = {0};
        int count;
        do {
            count = archive.listRecordings(next[0], 100, (control, correlation, id, startTime, stopTime, start, stop,
                initialTermId, segmentLength, termLength, mtu, session, recordedStream, stripped, original, identity) -> {
                next[0] = id + 1;
                if (recordedStream == streamId && "spine-output".equals(ChannelUri.parse(original).get("alias"))) {
                    candidates.add(new Span(id, session, start, stop, 0, 0, stop == AeronArchive.NULL_POSITION));
                }
            });
        } while (count == 100);
        candidates.sort(Comparator.comparingLong(Span::recordingId));
        final Long2ObjectHashMap<byte[]> hashes = new Long2ObjectHashMap<>();
        final Wire wire = new Wire();
        final MessageDigest digest = ContiguousEventDispatcher.newDigest();
        final byte[] scratch = new byte[Wire.MAX_EVENT_BYTES];
        final List<Span> spans = new ArrayList<>();
        long head = 0;
        for (final Span candidate : candidates) {
            final long[] range = {0, 0};
            final long start = archive.getStartPosition(candidate.recordingId());
            long complete = start;
            final long end = recordingPosition(archive, candidate.recordingId());
            if (end < complete || ((end | complete) & 31) != 0) { throw new IllegalStateException("invalid recording fence"); }
            if (end > complete) {
                try (BoundedReplay replay = new BoundedReplay(archive, candidate.recordingId(), complete,
                    end, replayChannel, replayStreamId, false)) {
                    while (!replay.done()) {
                        final int work = replay.poll((buffer, offset, length, header) -> {
                            if (!wire.validEvent(buffer, offset, length) || wire.event.logId() != logId) {
                                throw new IllegalStateException("invalid canonical recording or foreign log");
                            }
                            final long sequence = wire.event.sequence();
                            if (range[1] != 0 && (range[1] == Long.MAX_VALUE || sequence != range[1] + 1)) {
                                throw new IllegalStateException("noncontiguous output recording");
                            }
                            if (range[0] == 0) { range[0] = sequence; }
                            range[1] = sequence;
                            if (sequence <= lastApplied) { return; }
                            buffer.getBytes(offset, scratch, 0, length);
                            digest.update(scratch, 0, length);
                            final byte[] hash = digest.digest();
                            final byte[] prior = hashes.putIfAbsent(sequence, hash);
                            if (prior != null && !Arrays.equals(prior, hash)) {
                                throw new IllegalStateException("conflicting canonical recording overlap");
                            }
                        }, 100);
                        if (work == 0) { LockSupport.parkNanos(100_000); }
                    }
                    if (!candidate.active() && replay.unfinished()) { throw new IllegalStateException("incomplete closed recording"); }
                    complete = replay.completePosition();
                }
            }
            spans.add(new Span(candidate.recordingId(), candidate.sessionId(), start,
                complete, range[0], range[1], candidate.active()));
            head = Math.max(head, range[1]);
        }
        if (lastApplied > head) { throw new IllegalStateException("unavailable history: checkpoint beyond recorded head"); }
        if (hashes.size() != head - lastApplied) { throw new IllegalStateException("unavailable history: retained canonical suffix has a gap"); }
        return new RecordingCatalog(logId, spans, head);
    }

    private static long recordingPosition(final AeronArchive archive, final long id) {
        final long deadline = System.nanoTime() + archive.context().messageTimeoutNs();
        while (true) {
            final long position = archive.getRecordingPosition(id);
            if (position != AeronArchive.NULL_POSITION) { return position; }
            final long stop = archive.getStopPosition(id);
            if (stop != AeronArchive.NULL_POSITION) { return stop; }
            if (System.nanoTime() >= deadline) { throw new IllegalStateException("recording position unavailable"); }
            LockSupport.parkNanos(100_000);
        }
    }

    /** Select one recording containing the required next sequence, preferring the longest verified range. */
    public Span select(final long nextSequence) {
        Span selected = null;
        for (final Span span : spans) {
            final boolean containsNext = span.firstSequence() <= nextSequence && span.lastSequence() >= nextSequence;
            final boolean followsHead = nextSequence == head + 1 && span.active()
                && span.lastSequence() == head && (head != 0 || span.firstSequence() == 0);
            if (!containsNext && !followsHead) { continue; }
            final boolean extendsRange = selected == null || span.lastSequence() > selected.lastSequence();
            final boolean activeTie = selected != null && span.lastSequence() == selected.lastSequence()
                && span.active() && !selected.active();
            if (extendsRange || activeTie) { selected = span; }
        }
        return selected;
    }

    public long logId() { return logId; }
    public long headSequence() { return head; }
    public List<Span> spans() { return spans; }
}
