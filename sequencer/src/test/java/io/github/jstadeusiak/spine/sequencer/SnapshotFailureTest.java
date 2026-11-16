package io.github.jstadeusiak.spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import org.agrona.concurrent.status.CountersReader;
import java.util.concurrent.locks.LockSupport;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.*;

class SnapshotFailureTest {
    @TempDir Path directory;

    @Test @Timeout(120)
    void stoppedRecorderAndClosedSnapshotPublicationCannotAcceptSnapshotAndReplayRecovers() {
        for (final boolean stopRecorder : new boolean[]{true, false}) {
            final Path state = directory.resolve(Boolean.toString(stopRecorder));
            final Wire wire = new Wire();
            final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            final int initLength = wire.encodeInitialize(init, 0, 42);
            final UnsafeBuffer payload = new UnsafeBuffer(new byte[8]);
            final UnsafeBuffer proposal = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + 8]);
            final int length = wire.encodeProposal(proposal, 0, 42, 17, 1, 1, 1, 1, 2, 3, 4, payload, 0, 8);
            final FaultService service = new FaultService(stopRecorder);
            final ClusterFixture fixture = new ClusterFixture(state, service, true);
            final byte[] accepted;
            final String expected = stopRecorder ? "output recording stopped before snapshot fence" : "recorded publication unavailable";
            try {
                fixture.connect();
                fixture.exchange(init, initLength);
                accepted = fixture.exchange(proposal, length);
                assertThatThrownBy(fixture::takeSnapshot).hasStackTraceContaining(expected);
                try (RecordingLog log = new RecordingLog(fixture.clusterDirectory().toFile(), false)) {
                    assertThat(log.getLatestSnapshot(0)).as("failed service snapshot must not be accepted").isNull();
                }
            } finally {
                assertThatThrownBy(fixture::close).hasStackTraceContaining(expected);
            }
            try (ClusterFixture recovered = new ClusterFixture(state, new SequencerService(), false)) {
                recovered.connect();
                final byte[] retry = recovered.exchange(proposal, length);
                assertThat(wire.validResult(new UnsafeBuffer(retry), 0, retry.length)).isTrue();
                assertThat(wire.result.status()).isEqualTo(AdmissionStatus.DUPLICATE);
                assertThat(Arrays.copyOfRange(retry, Wire.RESULT_OVERHEAD, retry.length))
                    .isEqualTo(Arrays.copyOfRange(accepted, Wire.RESULT_OVERHEAD, accepted.length));
            }
        }
    }

    /** Test-only failure injection at the real snapshot callback; no production hooks or replacement transport. */
    private static final class FaultService implements ClusteredService {
        private final SequencerService delegate = new SequencerService();
        private final boolean stopRecorder;
        private Cluster cluster;
        FaultService(final boolean stopRecorder) { this.stopRecorder = stopRecorder; }
        @Override public void onTakeSnapshot(final ExclusivePublication publication) {
            if (stopRecorder) {
                final long recordingId = (long)field("recordingId");
                try (AeronArchive archive = AeronArchive.connect(cluster.context().archiveContext().clone()
                    .aeron(cluster.aeron()).ownsAeronClient(false))) {
                    if (!archive.tryStopRecordingByIdentity(recordingId)) { throw new AssertionError("recording not active"); }
                }
                final CountersReader counters = (CountersReader)field("counters");
                final int counter = (int)field("recordingCounter");
                final long deadline = System.nanoTime() + 5_000_000_000L;
                while (RecordingPos.isActive(counters, counter, recordingId)) {
                    if (System.nanoTime() >= deadline) { throw new AssertionError("recording stop did not complete"); }
                    LockSupport.parkNanos(100_000);
                }
            } else {
                publication.close();
            }
            delegate.onTakeSnapshot(publication);
        }
        private Object field(final String name) {
            try {
                final Field field = SequencerService.class.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(delegate);
            } catch (final ReflectiveOperationException failure) { throw new AssertionError(failure); }
        }
        @Override public void onStart(final Cluster cluster, final Image snapshot) {
            this.cluster = cluster;
            delegate.onStart(cluster, snapshot);
        }
        @Override public void onSessionMessage(final ClientSession session, final long timestamp,
            final DirectBuffer buffer, final int offset, final int length, final Header header) {
            delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
        @Override public void onTerminate(final Cluster cluster) { delegate.onTerminate(cluster); }
        @Override public void onSessionOpen(final ClientSession session, final long timestamp) { delegate.onSessionOpen(session, timestamp); }
        @Override public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason reason) {
            delegate.onSessionClose(session, timestamp, reason);
        }
        @Override public void onTimerEvent(final long correlation, final long timestamp) { delegate.onTimerEvent(correlation, timestamp); }
        @Override public void onRoleChange(final Cluster.Role role) { delegate.onRoleChange(role); }
    }
}
