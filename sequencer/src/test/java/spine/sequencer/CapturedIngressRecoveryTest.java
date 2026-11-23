package spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import spine.feeds.CapturedIngress;
import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real recording/replay boundaries, including transport fragments that never formed an admissible proposal. */
@Timeout(60)
class CapturedIngressRecoveryTest {
    private static final long LOG = 42, PRODUCER = 10;
    private static final int SOURCE = 7, CAPTURE_STREAM = 7951, REPLAY_STREAM = 8951;
    private static final String ALIAS = "spine-capture-10";
    private static final String CHANNEL = "aeron:ipc?term-length=1m|alias=" + ALIAS;
    @TempDir Path directory;

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void finalIncompleteTailIsDiscardedAcrossRepeatedRestarts(final boolean completePrefix) {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true);
             AeronArchive archive = fixture.connectArchive(9951)) {
            initialize(fixture);
            fixture.expectClosedConsumerControlResponse(archive);
            final long recording, completePosition;
            try (Capture capture = new Capture(fixture, archive)) {
                if (completePrefix) { capture.write(proposal(1)); }
                completePosition = capture.position;
                capture.beginOnly(proposal(completePrefix ? 2 : 1));
                recording = capture.recording;
                assertThat(capture.position).isGreaterThan(completePosition);
            }
            final int[] recovered = {0};
            for (int restart = 0; restart < 2; restart++) {
                try (CapturedIngress ingress = new CapturedIngress(archive, fixture.producerContext(), LOG, PRODUCER,
                    SOURCE, CAPTURE_STREAM, REPLAY_STREAM, (bytes, offset, length, header) -> {
                        assertThat(header).isNull();
                        final Wire wire = new Wire();
                        assertThat(wire.validProposal(bytes, offset, length)).isTrue();
                        assertThat(wire.proposal.producerSequence()).isEqualTo(1);
                        recovered[0]++;
                    })) {
                    assertThat(archive.getStopPosition(recording)).isEqualTo(completePosition);
                    assertThat(ingress.idle()).isEqualTo(!completePrefix);
                    fixture.await(ingress::idle, ingress::poll, "recovered complete proposal admission");
                    assertThat(ingress.acknowledgedSequence()).isEqualTo(completePrefix ? 1 : 0);
                }
                final long generation = latestRecording(archive);
                fixture.await(() -> archive.getStopPosition(generation) != AeronArchive.NULL_POSITION,
                    () -> { }, "recovery generation stopped");
            }
            assertThat(recovered[0]).isEqualTo(completePrefix ? 2 : 0);
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void earlierIncompleteGenerationFailsEvenBeforeAnEmptyLaterGeneration(final boolean laterProposal) {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true);
             AeronArchive archive = fixture.connectArchive(9952)) {
            fixture.expectClosedConsumerControlResponse(archive);
            final long first, end;
            try (Capture capture = new Capture(fixture, archive)) {
                capture.write(proposal(1));
                capture.beginOnly(proposal(2));
                first = capture.recording;
                end = capture.position;
            }
            try (Capture capture = new Capture(fixture, archive)) {
                if (laterProposal) { capture.write(proposal(2)); }
            }
            final int[] recovered = {0};
            assertThatThrownBy(() -> new CapturedIngress(archive, fixture.producerContext(), LOG, PRODUCER,
                SOURCE, CAPTURE_STREAM, REPLAY_STREAM, (bytes, offset, length, header) -> recovered[0]++))
                .isInstanceOf(IllegalStateException.class).hasMessage("incomplete prior capture generation");
            assertThat(recovered[0]).isEqualTo(1);
            assertThat(archive.getStopPosition(first)).as("unsafe prior history is not truncated").isEqualTo(end);
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    @ParameterizedTest @EnumSource(BadCapture.class)
    void malformedGapDuplicateOrForeignCompleteCaptureFails(final BadCapture bad) {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true);
             AeronArchive archive = fixture.connectArchive(9953)) {
            fixture.expectClosedConsumerControlResponse(archive);
            try (Capture capture = new Capture(fixture, archive)) { capture.write(proposal(1)); }
            final long sequence = bad == BadCapture.GAP ? 3 : bad == BadCapture.DUPLICATE ? 1 : 2;
            final UnsafeBuffer invalid = proposal(sequence, bad == BadCapture.LOG ? LOG + 1 : LOG,
                bad == BadCapture.PRODUCER ? PRODUCER + 1 : PRODUCER, bad == BadCapture.SOURCE ? SOURCE + 1 : SOURCE);
            if (bad == BadCapture.MALFORMED) { invalid.putShort(2, (short)99, Wire.ORDER); }
            assertThat(new Wire().validProposal(invalid, 0, invalid.capacity())).isEqualTo(bad != BadCapture.MALFORMED);
            try (Capture capture = new Capture(fixture, archive)) { capture.write(invalid); }
            final int[] recovered = {0};
            assertThatThrownBy(() -> new CapturedIngress(archive, fixture.producerContext(), LOG, PRODUCER,
                SOURCE, CAPTURE_STREAM, REPLAY_STREAM, (bytes, offset, length, header) -> recovered[0]++))
                .isInstanceOf(IllegalStateException.class).hasMessage("invalid or noncontiguous capture proposal");
            assertThat(recovered[0]).isEqualTo(1);
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    @Test
    void recoveryCallbackFailureStopsFurtherEffectsAndReachesCallerOutsideImagePoll() {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true);
             AeronArchive archive = fixture.connectArchive(9954)) {
            fixture.expectClosedConsumerControlResponse(archive);
            try (Capture capture = new Capture(fixture, archive)) {
                capture.write(proposal(1));
                capture.write(proposal(2));
            }
            final IllegalStateException failure = new IllegalStateException("source prefix differs");
            final int[] callbacks = {0};
            assertThatThrownBy(() -> new CapturedIngress(archive, fixture.producerContext(), LOG, PRODUCER,
                SOURCE, CAPTURE_STREAM, REPLAY_STREAM, (bytes, offset, length, header) -> {
                    callbacks[0]++;
                    throw failure;
                })).isSameAs(failure);
            assertThat(callbacks[0]).isEqualTo(1);
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    @Test
    void activeCaptureOwnerIsRejectedWithinItsShortColdTimeout() {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true);
             AeronArchive archive = fixture.connectArchive(9955)) {
            try (Capture capture = new Capture(fixture, archive)) {
                capture.write(proposal(1));
                final long timeout = archive.context().messageTimeoutNs();
                archive.context().messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(250));
                try {
                    final long before = System.nanoTime();
                    assertThatThrownBy(() -> new CapturedIngress(archive, fixture.producerContext(), LOG, PRODUCER,
                        SOURCE, CAPTURE_STREAM, REPLAY_STREAM, null))
                        .isInstanceOf(IllegalStateException.class).hasMessage("previous capture owner still active");
                    assertThat(System.nanoTime() - before).isLessThan(TimeUnit.SECONDS.toNanos(5));
                    assertThat(archive.getStopPosition(capture.recording)).isEqualTo(AeronArchive.NULL_POSITION);
                    assertThat(latestRecording(archive)).as("no new writer opened").isEqualTo(capture.recording);
                } finally { archive.context().messageTimeoutNs(timeout); }
            }
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    private static void initialize(final ClusterFixture fixture) {
        fixture.connect();
        final Wire wire = new Wire();
        final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        fixture.exchange(init, wire.encodeInitialize(init, 0, LOG));
    }

    private static UnsafeBuffer proposal(final long sequence) { return proposal(sequence, LOG, PRODUCER, SOURCE); }

    private static UnsafeBuffer proposal(final long sequence, final long log, final long producer, final int source) {
        final UnsafeBuffer raw = new UnsafeBuffer(new byte[] {'{', '}'});
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[CaptureCodec.OBSERVATION_OVERHEAD + raw.capacity()]);
        final int size = new CaptureCodec().encodeObservation(payload, 0, sequence, 0, sequence,
            null, 0, 0, raw, 0, raw.capacity());
        final UnsafeBuffer proposal = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + size]);
        new Wire().encodeProposal(proposal, 0, log, producer, sequence, source, 11, 13,
            100, 101, 102, payload, 0, size);
        return proposal;
    }

    private static long latestRecording(final AeronArchive archive) {
        final long[] latest = {-1};
        archive.listRecordingsForUri(0, 100, ALIAS, CAPTURE_STREAM,
            (control, correlation, id, startTime, stopTime, start, stop, termId, segment, term,
                mtu, session, stream, stripped, original, identity) -> latest[0] = Math.max(latest[0], id));
        assertThat(latest[0]).isNotNegative();
        return latest[0];
    }

    private enum BadCapture { MALFORMED, GAP, DUPLICATE, LOG, PRODUCER, SOURCE }

    private static final class Capture implements AutoCloseable {
        final ClusterFixture fixture;
        final AeronArchive archive;
        final ExclusivePublication publication;
        final CountersReader counters;
        final int counter;
        final long recording;
        long position;

        Capture(final ClusterFixture fixture, final AeronArchive archive) {
            this.fixture = fixture;
            this.archive = archive;
            publication = archive.addRecordedExclusivePublication(CHANNEL, CAPTURE_STREAM);
            counters = archive.context().aeron().countersReader();
            final int[] found = {CountersReader.NULL_COUNTER_ID};
            fixture.await(() -> (found[0] = RecordingPos.findCounterIdBySession(counters,
                publication.sessionId(), archive.archiveId())) != CountersReader.NULL_COUNTER_ID,
                () -> { }, "capture fixture recorder");
            counter = found[0];
            recording = RecordingPos.getRecordingId(counters, counter);
            position = publication.position();
        }

        void write(final UnsafeBuffer bytes) {
            fixture.await(() -> (position = publication.offer(bytes, 0, bytes.capacity())) > 0,
                () -> { }, "complete capture fixture offer");
            recorded();
        }

        void beginOnly(final UnsafeBuffer bytes) {
            // A committed BEGIN frame with no END models a process dying before the complete capture fence.
            final BufferClaim claim = new BufferClaim();
            fixture.await(() -> (position = publication.tryClaim(32, claim)) > 0,
                () -> { }, "partial capture fixture claim");
            claim.putBytes(bytes, 0, 32).flags(FrameDescriptor.BEGIN_FRAG_FLAG).commit();
            recorded();
        }

        void recorded() {
            fixture.await(() -> RecordingPos.isActive(counters, counter, recording)
                && counters.getCounterValue(counter) >= position, () -> { }, "capture fixture recorded bytes");
        }

        @Override public void close() {
            publication.close();
            fixture.await(() -> archive.getStopPosition(recording) != AeronArchive.NULL_POSITION,
                () -> { }, "capture fixture stopped");
        }
    }
}
