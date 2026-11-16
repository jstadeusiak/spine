package io.github.jstadeusiak.spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.protocol.ApplicationIdentity;
import io.github.jstadeusiak.spine.protocol.ApplicationLimits;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import io.github.jstadeusiak.spine.protocol.sbe.ObservationProposalDecoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApplicationIdentityTest {
    private static final long LOG = 42;
    private static final ApplicationIdentity IDENTITY = new ApplicationIdentity(502, 1, 3);

    @Test
    void eachLoggedIdentityFieldIsBoundBeforeInitializationOrApplicationMutation() {
        final State application = new State(8);
        final SequencerCore core = new SequencerCore(IDENTITY, application);
        final ApplicationIdentity[] different = {
            new ApplicationIdentity(503, 1, 3), new ApplicationIdentity(502, 2, 3),
            new ApplicationIdentity(502, 1, 4)
        };
        for (final ApplicationIdentity mismatch : different) {
            assertThatThrownBy(() -> send(core, initialize(mismatch)))
                .isInstanceOf(IllegalStateException.class);
            assertThat(core.logId()).isZero();
            assertThat(core.sequence()).isZero();
            assertThat(application.sequence).isZero();
        }
        assertThat(send(core, initialize(IDENTITY))).isEqualTo(AdmissionStatus.INITIALIZED);
        assertThat(send(core, proposal(1, 1))).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] saved = snapshot(core);
        for (final ApplicationIdentity mismatch : different) {
            assertThatThrownBy(() -> send(core, initialize(mismatch)))
                .isInstanceOf(IllegalStateException.class);
            assertThat(snapshot(core)).isEqualTo(saved);
            assertThat(core.application()).isSameAs(application);
        }
        assertThat(send(core, initialize(IDENTITY))).isEqualTo(AdmissionStatus.INITIALIZED);
        assertThat(send(core, proposal(1, 1))).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(application.sequence).isEqualTo(1);
    }

    @Test
    void unsignedProvisionedIdsRemainLosslessInInitializationAndSnapshot() {
        final ApplicationIdentity high = new ApplicationIdentity(Integer.MIN_VALUE, 2, Integer.MIN_VALUE + 1);
        final SequencerCore original = new SequencerCore(high, new State(8));
        assertThat(send(original, initialize(high))).isEqualTo(AdmissionStatus.INITIALIZED);
        assertThat(send(original, proposal(1, 1))).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] saved = snapshot(original);
        final SequencerCore recovered = new SequencerCore(high, new State(8));
        recovered.restore(new UnsafeBuffer(saved), 0, saved.length);
        assertThat(snapshot(recovered)).isEqualTo(saved);
        assertThat(send(recovered, proposal(1, 1))).isEqualTo(AdmissionStatus.DUPLICATE);
    }

    @Test
    void snapshotIdentityMismatchRejectsBeforeCallingApplicationRestoreEvenWithAValidDigest() {
        final State application = new State(8);
        final SequencerCore core = initialized(application);
        assertThat(send(core, proposal(1, 1))).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] saved = snapshot(core);
        for (final int offset : new int[]{24, 28, 32}) {
            final UnsafeBuffer mismatch = new UnsafeBuffer(saved.clone());
            mismatch.putInt(offset, mismatch.getInt(offset, Wire.ORDER) + 1, Wire.ORDER);
            SnapshotChecksum.seal(mismatch);
            assertThatThrownBy(() -> core.restore(mismatch, 0, mismatch.capacity()))
                .isInstanceOf(IllegalArgumentException.class);
            assertThat(application.restoreCalls).isZero();
            assertThat(core.application()).isSameAs(application);
            assertThat(snapshot(core)).isEqualTo(saved);
        }
    }

    @Test
    void everySnapshotByteIsCoveredBeforeCandidateRestoreAndFailedRestorePreservesExactRetry() {
        final State application = new State(8);
        final SequencerCore core = initialized(application);
        final UnsafeBuffer proposal = proposal(1, 1);
        assertThat(send(core, proposal)).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] event = canonical(core);
        final byte[] saved = snapshot(core);
        for (int i = 0; i < saved.length; i++) {
            final UnsafeBuffer corrupt = new UnsafeBuffer(saved.clone());
            corrupt.putByte(i, (byte)(corrupt.getByte(i) ^ 1));
            assertThatThrownBy(() -> core.restore(corrupt, 0, corrupt.capacity()))
                .as("snapshot byte %s", i).isInstanceOf(IllegalArgumentException.class);
            assertThat(application.restoreCalls).isZero();
            assertThat(core.application()).isSameAs(application);
            assertThat(snapshot(core)).isEqualTo(saved);
        }
        assertThat(send(core, proposal)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(canonical(core)).isEqualTo(event);
        assertThat(application.sequence).isEqualTo(1);
    }

    @Test
    void mixingIndividuallyValidSnapshotCutsCannotReachApplicationRestoreWithoutANewDigest() {
        final State application = new State(8);
        final SequencerCore core = initialized(application);
        assertThat(send(core, proposal(1, 1))).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] earlier = snapshot(core);
        assertThat(send(core, proposal(1, 2))).isEqualTo(AdmissionStatus.ACCEPTED);
        final byte[] later = snapshot(core);
        final UnsafeBuffer mixed = new UnsafeBuffer(earlier.clone());
        final int stateOffset = Wire.SNAPSHOT_OVERHEAD + Wire.RECEIPT_OVERHEAD + 4;
        mixed.putBytes(stateOffset, later, stateOffset, 8);
        assertThatThrownBy(() -> core.restore(mixed, 0, mixed.capacity()))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(application.restoreCalls).isZero();
        assertThat(snapshot(core)).isEqualTo(later);
        SnapshotChecksum.seal(mixed);
        assertThatThrownBy(() -> core.restore(mixed, 0, mixed.capacity()))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(application.restoreCalls).isEqualTo(1);
        assertThat(core.application()).isSameAs(application);
        assertThat(snapshot(core)).isEqualTo(later);
    }

    @Test
    void bothNativeContextsRequireTheExactSemanticsVersionBeforeLaunch() {
        final ApplicationIdentity identity = new ApplicationIdentity(502, 2, 3);
        final SequencerService service = new SequencerService(identity, new State(8));
        final ConsensusModule.Context consensus = new ConsensusModule.Context();
        final ClusteredServiceContainer.Context container = new ClusteredServiceContainer.Context();
        service.configure(consensus, container);
        assertThat(consensus.appVersion()).isEqualTo(2);
        assertThat(container.appVersion()).isEqualTo(2);
        assertThat(consensus.appVersionValidator().isVersionCompatible(2, 2)).isTrue();
        assertThat(container.appVersionValidator().isVersionCompatible(2, 2)).isTrue();
        assertThat(consensus.appVersionValidator().isVersionCompatible(2, 1)).isFalse();
        assertThat(container.appVersionValidator().isVersionCompatible(2, 1)).isFalse();
        assertThat(consensus.appVersionValidator().isVersionCompatible(2, 3)).isFalse();
        assertThat(container.appVersionValidator().isVersionCompatible(2, 3)).isFalse();

        final Cluster wrongContext = (Cluster)Proxy.newProxyInstance(Cluster.class.getClassLoader(),
            new Class<?>[]{Cluster.class}, (proxy, method, arguments) -> {
                if (method.getName().equals("context")) { return container.appVersion(1); }
                throw new AssertionError("service accessed resources before checking its native version");
            });
        assertThatThrownBy(() -> service.onStart(wrongContext, null))
            .isInstanceOf(AgentTerminationException.class).hasMessageContaining("version");
    }

    @Test @Timeout(90)
    void nativeSnapshotVersionMismatchStopsRecoveryBeforeApplicationRestore(@TempDir final Path directory) {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(IDENTITY, new State(8)), true)) {
            fixture.connect();
            exchange(fixture, initialize(IDENTITY), AdmissionStatus.INITIALIZED);
            exchange(fixture, proposal(1, 1), AdmissionStatus.ACCEPTED);
            fixture.takeSnapshot();
            fixture.abort();
        }
        final ApplicationIdentity changed = new ApplicationIdentity(IDENTITY.id(), 2, IDENTITY.configurationId());
        final State application = new State(8);
        assertThatThrownBy(() -> {
            try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(changed, application), false)) {
                fixture.await(() -> false, () -> { }, "native snapshot version rejection");
            }
        }).isInstanceOf(RuntimeException.class).hasStackTraceContaining("incompatible")
            .hasStackTraceContaining("snapshot=0.0.1");
        assertThat(application.restoreCalls).isZero();
        assertThat(application.sequence).isZero();
    }

    @Test @Timeout(90)
    void snapshotLargerThanOneNativeMessageRestoresAllReceipts(@TempDir final Path directory) {
        final SnapshotService initial = new SnapshotService();
        final UnsafeBuffer last = proposal(Wire.MAX_PRODUCERS, 1);
        final byte[] lastEvent;
        try (ClusterFixture fixture = new ClusterFixture(directory, initial, true)) {
            fixture.connect();
            exchange(fixture, initialize(IDENTITY), AdmissionStatus.INITIALIZED);
            for (int producer = 1; producer < Wire.MAX_PRODUCERS; producer++) {
                exchange(fixture, proposal(producer, 1), AdmissionStatus.ACCEPTED);
            }
            lastEvent = exchange(fixture, last, AdmissionStatus.ACCEPTED);
            fixture.takeSnapshot();
            assertThat(initial.snapshotMessageBound).isLessThan(SnapshotService.STATE_BYTES);
            assertThat(initial.application.snapshotSequence).isEqualTo(Wire.MAX_PRODUCERS);
            assertThat(initial.application.snapshotLength).isEqualTo(SnapshotService.STATE_BYTES);
            fixture.abort();
        }
        final SnapshotService recovered = new SnapshotService();
        try (ClusterFixture fixture = new ClusterFixture(directory, recovered, false)) {
            fixture.connect();
            assertThat(recovered.application.restoreCalls).isEqualTo(1);
            assertThat(exchange(fixture, last, AdmissionStatus.DUPLICATE)).isEqualTo(lastEvent);
            exchange(fixture, proposal(1, 1), AdmissionStatus.DUPLICATE);
            exchange(fixture, proposal(Wire.MAX_PRODUCERS + 1, 1), AdmissionStatus.CAPACITY);
            exchange(fixture, proposal(1, 2), AdmissionStatus.ACCEPTED);
        }
    }

    private static SequencerCore initialized(final State application) {
        final SequencerCore core = new SequencerCore(IDENTITY, application);
        assertThat(send(core, initialize(IDENTITY))).isEqualTo(AdmissionStatus.INITIALIZED);
        return core;
    }

    private static UnsafeBuffer initialize(final ApplicationIdentity identity) {
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        new Wire().encodeInitialize(input, 0, LOG, identity);
        return input;
    }

    private static UnsafeBuffer proposal(final long producer, final long sequence) {
        final UnsafeBuffer payload = SequencerCoreTest.payload(7, 16);
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payload.capacity()]);
        new Wire().encodeProposal(input, 0, LOG, producer, sequence, 1, 2, 3, 10, 11, 12,
            payload, 0, payload.capacity());
        return input;
    }

    private static AdmissionStatus send(final SequencerCore core, final UnsafeBuffer input) {
        return core.onCommand(input, 0, input.capacity(), 100, 1);
    }

    private static byte[] snapshot(final SequencerCore core) {
        return SequencerCoreTest.bytes(core.resultBuffer(), 0, core.snapshot());
    }

    private static byte[] canonical(final SequencerCore core) {
        return SequencerCoreTest.bytes(core.resultBuffer(), Wire.RESULT_OVERHEAD,
            core.resultLength() - Wire.RESULT_OVERHEAD);
    }

    private static byte[] exchange(final ClusterFixture fixture, final UnsafeBuffer input,
        final AdmissionStatus expected) {
        final byte[] reply = fixture.exchange(input, input.capacity());
        final Wire wire = new Wire();
        assertThat(wire.validResult(new UnsafeBuffer(reply), 0, reply.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(expected);
        return Arrays.copyOfRange(reply, Wire.RESULT_OVERHEAD, reply.length);
    }

    private static final class State implements Application {
        private final int stateLength;
        private long sequence;
        private volatile int restoreCalls;
        private volatile int snapshotLength;
        private volatile long snapshotSequence;

        private State(final int stateLength) { this.stateLength = stateLength; }

        @Override public int apply(final DirectBuffer input, final int offset, final int length,
            final ObservationProposalDecoder proposal, final long timeNanos,
            final MutableDirectBuffer output, final int outputOffset) {
            sequence++;
            return 0;
        }

        @Override public int snapshot(final MutableDirectBuffer output, final int offset) {
            output.setMemory(offset, stateLength, (byte)0);
            output.putLong(offset, sequence, Wire.ORDER);
            snapshotLength = stateLength;
            snapshotSequence = sequence;
            return stateLength;
        }

        @Override public Application restore(final DirectBuffer input, final int offset, final int length,
            final long canonicalSequence) {
            restoreCalls++;
            if (length != stateLength || input.getLong(offset, Wire.ORDER) != canonicalSequence) {
                throw new IllegalArgumentException("state disagrees with snapshot frontier");
            }
            for (int i = 8; i < length; i++) {
                if (input.getByte(offset + i) != 0) { throw new IllegalArgumentException("invalid state padding"); }
            }
            final State candidate = new State(stateLength);
            candidate.sequence = canonicalSequence;
            return candidate;
        }

        @Override public void validateResult(final DirectBuffer input, final int offset, final int length) {
            if (length != 0) { throw new IllegalArgumentException("unexpected cached result"); }
        }
    }

    private static final class SnapshotService implements ClusteredService {
        private static final int STATE_BYTES = 8 * 1024 * 1024;
        private final State application = new State(STATE_BYTES);
        private final SequencerService delegate = new SequencerService(IDENTITY, application,
            new ApplicationLimits(Wire.MAX_PRODUCERS, ApplicationLimits.DEFAULT.resultBytes(), STATE_BYTES),
            SequencerService.OUTPUT_CHANNEL, SequencerService.OUTPUT_STREAM);
        private volatile int snapshotMessageBound;

        @Override public void onStart(final Cluster cluster, final Image snapshot) { delegate.onStart(cluster, snapshot); }
        @Override public void onTakeSnapshot(final ExclusivePublication publication) {
            snapshotMessageBound = publication.maxMessageLength();
            delegate.onTakeSnapshot(publication);
        }
        @Override public void onSessionMessage(final ClientSession session, final long timestamp,
            final DirectBuffer buffer, final int offset, final int length, final Header header) {
            delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
        @Override public void onTerminate(final Cluster cluster) { delegate.onTerminate(cluster); }
        @Override public void onSessionOpen(final ClientSession session, final long timestamp) {
            delegate.onSessionOpen(session, timestamp);
        }
        @Override public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason reason) {
            delegate.onSessionClose(session, timestamp, reason);
        }
        @Override public void onTimerEvent(final long correlationId, final long timestamp) {
            delegate.onTimerEvent(correlationId, timestamp);
        }
        @Override public void onRoleChange(final Cluster.Role role) { delegate.onRoleChange(role); }
    }
}
