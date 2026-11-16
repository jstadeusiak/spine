package io.github.jstadeusiak.spine.sequencer;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RecordingIdentityTest {
    @TempDir Path directory;

    @Test @Timeout(60)
    void startupWaitsForTheRecordingCounterRegistrationBeforeRetainingItsIdentity() throws Exception {
        final DelayedRegistration service = new DelayedRegistration();
        try (ClusterFixture fixture = new ClusterFixture(directory, service, true)) {
            try {
                assertThat(service.observed.await(10, TimeUnit.SECONDS)).as("startup reached the registration window").isTrue();
                assertThat(service.zeroRegistrationPolls).as("startup waits while the visible counter has no registration")
                    .isEqualTo(2);
                assertThat(service.started.getCount()).as("service cannot become ready before registration").isEqualTo(1);
            } finally {
                service.releaseRegistration();
            }
            assertThat(service.started.await(10, TimeUnit.SECONDS)).as("startup after registration publication").isTrue();
            fixture.assertHealthy();
            assertThat(service.field("recordingRegistration")).isEqualTo(service.registration);

            fixture.connect();
            final Wire wire = new Wire();
            final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            final byte[] initialized = fixture.exchange(init, wire.encodeInitialize(init, 0, 42));
            assertThat(wire.validResult(new UnsafeBuffer(initialized), 0, initialized.length)).isTrue();
            assertThat(wire.result.status()).isEqualTo(AdmissionStatus.INITIALIZED);
            final UnsafeBuffer payload = new UnsafeBuffer(new byte[8]);
            final UnsafeBuffer proposal = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + 8]);
            final int length = wire.encodeProposal(proposal, 0, 42, 17, 1, 1, 1, 1, 2, 3, 4, payload, 0, 8);
            final byte[] accepted = fixture.exchange(proposal, length);
            assertThat(wire.validResult(new UnsafeBuffer(accepted), 0, accepted.length)).isTrue();
            assertThat(wire.result.status()).isEqualTo(AdmissionStatus.ACCEPTED);
            fixture.takeSnapshot();
        }
    }

    /** Retain native transport, but expose the counter between ALLOCATED and registration publication. */
    private static final class DelayedRegistration implements ClusteredService {
        private final SequencerService delegate = new SequencerService();
        private final CountDownLatch observed = new CountDownLatch(1);
        private final CountDownLatch started = new CountDownLatch(1);
        private CountersReader nativeCounters;
        private CountersManager delayedCounters;
        private int counter;
        private long registration;
        private int zeroRegistrationPolls;

        @Override public void onStart(final Cluster cluster, final Image snapshot) {
            final IdleStrategy nativeIdle = cluster.idleStrategy();
            final IdleStrategy delayedIdle = new IdleStrategy() {
                @Override public void reset() {
                    nativeIdle.reset();
                    if (delayedCounters == null) { exposeUnregisteredCounter(nativeIdle); }
                }
                @Override public void idle() {
                    nativeIdle.idle();
                    if (delayedCounters.getCounterRegistrationId(counter) == CountersReader.DEFAULT_REGISTRATION_ID
                        && zeroRegistrationPolls < 2 && ++zeroRegistrationPolls == 2) {
                        observed.countDown();
                    }
                }
                @Override public void idle(final int workCount) {
                    if (workCount <= 0) { idle(); } else { nativeIdle.idle(workCount); }
                }
            };
            final Cluster wrapped = (Cluster)Proxy.newProxyInstance(Cluster.class.getClassLoader(),
                new Class<?>[]{Cluster.class}, (proxy, method, args) -> {
                    if (method.getName().equals("idleStrategy")) { return delayedIdle; }
                    try { return method.invoke(cluster, args); }
                    catch (final InvocationTargetException failure) { throw failure.getCause(); }
                });
            try {
                delegate.onStart(wrapped, snapshot);
            } finally {
                if (nativeCounters != null) { setField("counters", nativeCounters); }
                started.countDown();
                observed.countDown();
            }
        }

        private void exposeUnregisteredCounter(final IdleStrategy nativeIdle) {
            nativeCounters = (CountersReader)field("counters");
            final ExclusivePublication output = (ExclusivePublication)field("output");
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while ((counter = RecordingPos.findCounterIdBySession(nativeCounters, output.sessionId(), Aeron.NULL_VALUE))
                == CountersReader.NULL_COUNTER_ID
                || (registration = nativeCounters.getCounterRegistrationId(counter)) == CountersReader.DEFAULT_REGISTRATION_ID) {
                if (System.nanoTime() >= deadline) { throw new AssertionError("native recording identity was not published"); }
                nativeIdle.idle();
            }
            final UnsafeBuffer metadata = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                nativeCounters.metaDataBuffer().capacity(), 64));
            final UnsafeBuffer values = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                nativeCounters.valuesBuffer().capacity(), 64));
            metadata.putBytes(0, nativeCounters.metaDataBuffer(), 0, metadata.capacity());
            values.putBytes(0, nativeCounters.valuesBuffer(), 0, values.capacity());
            delayedCounters = new CountersManager(metadata, values);
            delayedCounters.setCounterRegistrationId(counter, CountersReader.DEFAULT_REGISTRATION_ID);
            setField("counters", delayedCounters);
        }

        private void releaseRegistration() {
            if (delayedCounters != null) { delayedCounters.setCounterRegistrationId(counter, registration); }
        }

        private Object field(final String name) {
            try {
                final Field field = SequencerService.class.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(delegate);
            } catch (final ReflectiveOperationException failure) { throw new AssertionError(failure); }
        }

        private void setField(final String name, final Object value) {
            try {
                final Field field = SequencerService.class.getDeclaredField(name);
                field.setAccessible(true);
                field.set(delegate, value);
            } catch (final ReflectiveOperationException failure) { throw new AssertionError(failure); }
        }

        @Override public void onSessionMessage(final ClientSession session, final long timestamp,
            final DirectBuffer buffer, final int offset, final int length, final Header header) {
            delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
        @Override public void onTakeSnapshot(final ExclusivePublication publication) { delegate.onTakeSnapshot(publication); }
        @Override public void onTerminate(final Cluster cluster) { delegate.onTerminate(cluster); }
        @Override public void onSessionOpen(final ClientSession session, final long timestamp) { delegate.onSessionOpen(session, timestamp); }
        @Override public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason reason) {
            delegate.onSessionClose(session, timestamp, reason);
        }
        @Override public void onTimerEvent(final long correlation, final long timestamp) { delegate.onTimerEvent(correlation, timestamp); }
        @Override public void onRoleChange(final Cluster.Role role) { delegate.onRoleChange(role); }
    }
}
