package io.github.jstadeusiak.spine.client;

import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.DirectBuffer;

/** One fixed physical replay or active-recording follow; borrowed Archive, owned subscription and assembly memory. */
final class BoundedReplay implements AutoCloseable {
    private final AeronArchive archive;
    private final Subscription subscription;
    private final long replayId;
    private final long endPosition;
    private final long timeoutNs;
    private final boolean follow;
    private final ImageFragmentAssembler assembler;
    private final FragmentHandler fragments = this::onFragment;
    private Image image;
    private FragmentHandler handler;
    private long deadline;
    private long completePosition;
    private boolean assembling;
    private int nextOffset;
    private int termId;
    private int messageBytes;
    private Throwable failure;

    BoundedReplay(final AeronArchive archive, final long recordingId, final long startPosition,
                  final long endPosition, final String replayChannel, final int replayStreamId, final boolean follow) {
        this.archive = archive;
        this.endPosition = endPosition;
        timeoutNs = archive.context().messageTimeoutNs();
        this.follow = follow;
        completePosition = startPosition;
        final ChannelUri uri = ChannelUri.parse(replayChannel);
        // Create the destination first so a small replay cannot finish before its subscription exists.
        subscription = archive.context().aeron().addSubscription(uri.toString(), replayStreamId);
        try {
            if (archive.getStartPosition(recordingId) > startPosition) {
                throw new IllegalStateException("unavailable history: replay prefix was purged; requalify the catalog");
            }
            replayId = archive.startReplay(recordingId, startPosition,
                follow ? AeronArchive.REPLAY_ALL_AND_FOLLOW : endPosition - startPosition,
                replayChannel, replayStreamId);
        } catch (final RuntimeException | Error error) { subscription.close(); throw error; }
        assembler = new ImageFragmentAssembler((buffer, offset, length, header) -> {
            handler.onFragment(buffer, offset, length, header);
            completePosition = header.position();
        }, Wire.MAX_EVENT_BYTES);
        deadline = System.nanoTime() + timeoutNs;
    }

    int poll(final FragmentHandler handler, final int fragmentLimit) {
        this.handler = handler;
        if (image == null) { image = subscription.imageBySessionId((int)replayId); }
        final int work = image == null ? 0 : image.poll(fragments, fragmentLimit);
        if (failure != null) { org.agrona.LangUtil.rethrowUnchecked(failure); }
        if (!follow && work > 0) { deadline = System.nanoTime() + timeoutNs; }
        if (image == null && System.nanoTime() >= deadline) { throw new IllegalStateException("replay image unavailable"); }
        if (!follow && !done() && ((image != null && image.isClosed()) || System.nanoTime() >= deadline)) {
            throw new IllegalStateException("bounded replay stopped before fence");
        }
        return work;
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        if (failure != null) { return; }
        try { consumeFragment(buffer, offset, length, header); }
        catch (final RuntimeException | Error error) { failure = error; }
    }

    private void consumeFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final byte flags = header.flags();
        if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED) {
            if (assembling) { throw new IllegalStateException("unfinished fragmented message"); }
            messageBytes = length;
        } else if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) != 0) {
            if (assembling) { throw new IllegalStateException("nested fragment"); }
            assembling = true;
            termId = header.termId();
            messageBytes = length;
        } else {
            if (!assembling || header.termOffset() != nextOffset || header.termId() != termId) {
                throw new IllegalStateException("discontinuous fragment");
            }
            messageBytes += length;
        }
        if (messageBytes > Wire.MAX_EVENT_BYTES) { throw new IllegalStateException("oversized canonical frame"); }
        nextOffset = header.nextTermOffset();
        assembler.onFragment(buffer, offset, length, header);
        if ((flags & FrameDescriptor.END_FRAG_FLAG) != 0) { assembling = false; }
    }

    boolean done() { return image != null && (follow ? image.isEndOfStream() || image.isClosed() : image.position() >= endPosition); }
    boolean unfinished() { return assembling; }
    long completePosition() { return completePosition; }

    @Override public void close() {
        try { archive.stopReplay(replayId); }
        catch (final ArchiveException error) {
            if (error.errorCode() != ArchiveException.UNKNOWN_REPLAY) { throw error; }
        } finally { subscription.close(); }
    }
}
