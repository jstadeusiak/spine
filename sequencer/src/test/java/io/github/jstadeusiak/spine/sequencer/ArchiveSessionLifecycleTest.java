package io.github.jstadeusiak.spine.sequencer;

import io.aeron.cluster.ConsensusModule;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ArchiveSessionLifecycleTest {
    @Test @Timeout(45)
    void outputRecordingAndSnapshotSurviveArchiveControlLivenessWithoutAnIdleSession(@TempDir final Path directory) {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true, false,
            ConsensusModule.Configuration.sessionTimeoutNs(), config -> {
                config.archiveContext().sessionLivenessCheckIntervalNs(TimeUnit.MILLISECONDS.toNanos(2))
                    .connectTimeoutNs(TimeUnit.MILLISECONDS.toNanos(300));
                config.clusteredServiceContext().archiveContext().controlResponseChannel("aeron:ipc?term-length=64k");
            })) {
            fixture.connect();
            final Wire wire = new Wire();
            final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.MAX_PROPOSAL_BYTES]);
            exchange(fixture, input, wire.encodeInitialize(input, 0, 42), AdmissionStatus.INITIALIZED);
            final UnsafeBuffer payload = SequencerCoreTest.payload(1, 16);
            exchange(fixture, input, wire.encodeProposal(input, 0, 42, 1, 1, 1, 1, 1, -1, -1, -1,
                payload, 0, payload.capacity()), AdmissionStatus.ACCEPTED);

            // More than 512 two-millisecond pings plus the 300 ms response deadline; await polls Cluster keepalives.
            final long idleUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            fixture.await(() -> System.nanoTime() >= idleUntil, () -> { }, "Archive control liveness interval");
            exchange(fixture, input, wire.encodeProposal(input, 0, 42, 1, 2, 1, 1, 1, -1, -1, -1,
                payload, 0, payload.capacity()), AdmissionStatus.ACCEPTED);
            fixture.takeSnapshot();
            fixture.shutdown();
        }
    }

    private static void exchange(final ClusterFixture fixture, final UnsafeBuffer input, final int length,
        final AdmissionStatus expected) {
        final byte[] reply = fixture.exchange(input, length);
        final Wire wire = new Wire();
        assertThat(wire.validResult(new UnsafeBuffer(reply), 0, reply.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(expected);
    }
}
