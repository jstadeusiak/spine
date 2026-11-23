package spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import spine.client.ReliableProducer;
import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/** A collector's exact capture precedes admission; transport progress remains live while admission is gated. */
class CollectorAdmissionTest {
    @TempDir Path directory;

    @Test @Timeout(60)
    void captureGateSuppressesOffersAndRetriesButAllowsKeepaliveAndReceiptProgress() {
        final ObservedService service = new ObservedService();
        try (ClusterFixture fixture = new ClusterFixture(directory, service, true)) {
            fixture.connect();
            final Wire wire = new Wire();
            final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            fixture.exchange(init, wire.encodeInitialize(init, 0, 42));
            try (ReliableProducer producer = new ReliableProducer(42, 10, 1);
                 AeronArchive archive = fixture.connectArchive(8901);
                 ExclusivePublication capture = archive.addRecordedExclusivePublication(
                     "aeron:ipc?term-length=1m|alias=spine-capture", 7901)) {
                producer.connect(fixture.producerContext());
                final UnsafeBuffer raw = new UnsafeBuffer(new byte[65_500]);
                final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
                final CaptureCodec codec = new CaptureCodec();
                final int length = codec.encodeObservation(payload, 0, 1, 0, 123,
                    null, 0, 0, raw, 0, raw.capacity());
                assertThat(producer.submit(7, 1, 13, 100, 101, 102, payload, 0, length)).isTrue();
                final byte[] exact = new byte[producer.pendingLength()];
                producer.pendingBuffer().getBytes(0, exact);

                // Longer than both the retry interval and the keepalive interval, with a populated slot.
                final long until = System.nanoTime() + 1_100_000_000L;
                int work = 0;
                while (System.nanoTime() - until < 0) {
                    work += producer.poll(false);
                    fixture.pollAdmin();
                    fixture.assertHealthy();
                    LockSupport.parkNanos(100_000);
                }
                assertThat(work).as("egress/keepalive progress with offers disabled").isPositive();
                assertThat(service.proposals).isZero();
                assertThat(producer.pendingLength()).isEqualTo(exact.length);
                assertThat(producer.nextSequence()).isEqualTo(1);

                final CountersReader counters = archive.context().aeron().countersReader();
                final int[] found = {CountersReader.NULL_COUNTER_ID};
                final long[] registered = {CountersReader.DEFAULT_REGISTRATION_ID};
                fixture.await(() -> (found[0] = RecordingPos.findCounterIdBySession(counters,
                    capture.sessionId(), archive.archiveId())) != CountersReader.NULL_COUNTER_ID
                    && (registered[0] = counters.getCounterRegistrationId(found[0])) != CountersReader.DEFAULT_REGISTRATION_ID,
                    () -> producer.poll(false), "capture recording counter");
                final int counter = found[0];
                final long recording = RecordingPos.getRecordingId(counters, counter);
                final long registration = registered[0];
                final long[] position = {0};
                fixture.await(() -> (position[0] = capture.offer(producer.pendingBuffer(), 0,
                    producer.pendingLength())) > 0, () -> producer.poll(false), "exact captured proposal");
                fixture.await(() -> complete(counters, counter, recording, registration, position[0]),
                    () -> producer.poll(false), "complete fragmented capture");
                assertThat(service.proposals).isZero();

                // Observe an actual committed proposal, withholding its egress until the retry gate is proved.
                fixture.await(() -> service.heldReply != null && service.proposals > 0,
                    () -> producer.poll(true), "committed proposal with held receipt");
                assertThat(wire.validResult(service.heldReply, 0, service.heldReply.capacity())).isTrue();
                assertThat(wire.result.status()).isEqualTo(AdmissionStatus.ACCEPTED);
                final int[] baseline = {service.proposals};
                final long[] quietUntil = {System.nanoTime() + 100_000_000L};
                fixture.await(() -> {
                    if (baseline[0] != service.proposals) {
                        baseline[0] = service.proposals;
                        quietUntil[0] = System.nanoTime() + 100_000_000L;
                    }
                    return System.nanoTime() - quietUntil[0] >= 0;
                }, () -> producer.poll(false), "already offered proposals drained");
                final long gatedUntil = System.nanoTime() + 100_000_000L;
                while (System.nanoTime() - gatedUntil < 0) {
                    producer.poll(false);
                    fixture.pollAdmin();
                    fixture.assertHealthy();
                    assertThat(producer.pendingLength()).isEqualTo(exact.length);
                    assertThat(service.proposals).as("no retry committed while gated").isEqualTo(baseline[0]);
                    LockSupport.parkNanos(100_000);
                }
                assertThat(producer.nextSequence()).isEqualTo(1);
                service.releaseReply = true;
                // Session sends are allowed in a logged callback, not doBackgroundWork.
                fixture.exchange(init, wire.encodeInitialize(init, 0, 42));
                fixture.await(() -> service.replyReleased && producer.pendingLength() == 0,
                    () -> producer.poll(false), "receipt while gated");
                assertThat(service.replyReleased).isTrue();
                assertThat(service.proposals).as("no retry was committed while gated").isEqualTo(baseline[0]);
                assertThat(producer.lastStatus()).isEqualTo(AdmissionStatus.ACCEPTED);
                assertThat(producer.nextSequence()).isEqualTo(2);
                assertThat(producer.acknowledgedSequence()).isEqualTo(1);
                assertThat(service.exactProposal).containsExactly(exact);
            }
            fixture.assertHealthy();
            fixture.abort();
        }
    }

    private static boolean complete(final CountersReader counters, final int counter, final long recording,
                                    final long registration, final long position) {
        if (counters.getCounterRegistrationId(counter) != registration
            || !RecordingPos.isActive(counters, counter, recording)) { return false; }
        final long recorded = counters.getCounterValue(counter);
        return counters.getCounterRegistrationId(counter) == registration
            && RecordingPos.isActive(counters, counter, recording) && recorded >= position;
    }

    private static final class ObservedService implements ClusteredService {
        final SequencerService delegate = new SequencerService();
        final Wire wire = new Wire();
        volatile int proposals;
        volatile byte[] exactProposal;
        volatile UnsafeBuffer heldReply;
        volatile boolean releaseReply;
        volatile boolean replyReleased;
        ClientSession replySession;
        @Override public void onStart(Cluster cluster, Image snapshot) { delegate.onStart(cluster, snapshot); }
        @Override public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer,
                                              int offset, int length, Header header) {
            if (releaseReply && !replyReleased && heldReply != null
                && replySession.offer(heldReply, 0, heldReply.capacity()) > 0) { replyReleased = true; }
            if (wire.validProposal(buffer, offset, length)) {
                exactProposal = new byte[length];
                buffer.getBytes(offset, exactProposal);
                proposals++;
                final ClientSession actualSession = session;
                session = (ClientSession)Proxy.newProxyInstance(ClientSession.class.getClassLoader(),
                    new Class<?>[]{ClientSession.class}, (proxy, method, args) -> {
                        if (method.getName().equals("offer") && args.length == 3) {
                            if (heldReply == null) {
                                final byte[] reply = new byte[(int)args[2]];
                                ((DirectBuffer)args[0]).getBytes((int)args[1], reply);
                                replySession = actualSession;
                                heldReply = new UnsafeBuffer(reply);
                            }
                            return Publication.BACK_PRESSURED;
                        }
                        try { return method.invoke(actualSession, args); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            }
            delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
        @Override public void onTakeSnapshot(ExclusivePublication publication) { delegate.onTakeSnapshot(publication); }
        @Override public void onTerminate(Cluster cluster) { delegate.onTerminate(cluster); }
        @Override public void onRoleChange(Cluster.Role role) { delegate.onRoleChange(role); }
        @Override public void onSessionOpen(ClientSession session, long timestamp) { delegate.onSessionOpen(session, timestamp); }
        @Override public void onSessionClose(ClientSession session, long timestamp, CloseReason reason) {
            delegate.onSessionClose(session, timestamp, reason);
        }
        @Override public void onTimerEvent(long correlation, long timestamp) { delegate.onTimerEvent(correlation, timestamp); }
    }
}
