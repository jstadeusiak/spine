package spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import spine.client.CatalogSubscription;
import spine.client.RecordingCatalog;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;

class SequencerIntegrationTest {
    private static final long LOG = 42;
    @TempDir Path directory;
    private final Wire wire = new Wire();

    @Test @Timeout(90)
    void oneCommitDirectOutputLostReceiptAndSnapshotSuffixReplayStayIdentical() {
        Command a = proposal(10, 1, 100, 4096), b = proposal(11, 1, 101, 4096);
        Command c = proposal(12, 1, 102, 4096), d = proposal(13, 1, 103, Wire.MAX_PAYLOAD_BYTES);
        byte[] first, second, third, fourth;
        ObservedService initial = new ObservedService();
        try (ClusterFixture fixture = new ClusterFixture(directory, initial, true)) {
            fixture.connect();
            assertResult(fixture.exchange(a.buffer, a.length), AdmissionStatus.UNINITIALIZED);
            initialize(fixture);
            first = fixture.exchange(a.buffer, a.length);
            second = fixture.exchange(b.buffer, b.length);
            assertResult(first, AdmissionStatus.ACCEPTED); assertResult(second, AdmissionStatus.ACCEPTED);
            fixture.takeSnapshot();
            fixture.discardNextReply();
            third = fixture.exchange(c.buffer, c.length);
            assertResult(third, AdmissionStatus.DUPLICATE);
            fourth = fixture.exchange(d.buffer, d.length);
            assertResult(fourth, AdmissionStatus.ACCEPTED);
            try (AeronArchive archive = fixture.connectArchive(8501)) {
                RecordingCatalog catalog = catalog(archive, 9501);
                assertThat(catalog.headSequence()).isEqualTo(4);
                List<byte[]> actual = collect(fixture, archive, 0, 4, 9502);
                assertThat(actual).containsExactly(canonical(first), canonical(second), canonical(third), canonical(fourth));
            }
            fixture.abort();
            assertThat(initial.selfOffers.get()).isZero();
        }
        ObservedService recovered = new ObservedService();
        try (ClusterFixture fixture = new ClusterFixture(directory, recovered, false)) {
            fixture.connect();
            assertThat(recovered.restored).isTrue();
            assertThat(recovered.replayed).as("only committed proposals after the snapshot replay").isEqualTo(3);
            assertThat(canonical(fixture.exchange(a.buffer, a.length))).isEqualTo(canonical(first));
            assertThat(canonical(fixture.exchange(d.buffer, d.length))).isEqualTo(canonical(fourth));
            Command next = proposal(10, 2, 104, 4096);
            byte[] fifth = fixture.exchange(next.buffer, next.length);
            assertResult(fifth, AdmissionStatus.ACCEPTED);
            try (AeronArchive archive = fixture.connectArchive(8601)) {
                RecordingCatalog catalog = catalog(archive, 9601);
                assertThat(catalog.headSequence()).isEqualTo(5);
                List<byte[]> actual = collect(fixture, archive, 0, 5, 9602);
                assertThat(actual).containsExactly(canonical(first), canonical(second), canonical(third), canonical(fourth), canonical(fifth));
                assertThat(collect(fixture, archive, 2, 5, 9603)).containsExactly(canonical(third), canonical(fourth), canonical(fifth));
            }
            assertThat(recovered.selfOffers.get()).isZero();
        }
    }

    private RecordingCatalog catalog(AeronArchive archive, int stream) {
        return RecordingCatalog.rebuild(archive, LOG, SequencerService.OUTPUT_STREAM, "aeron:ipc?term-length=1m", stream);
    }
    private List<byte[]> collect(ClusterFixture fixture, AeronArchive archive, long from, long through, int stream) {
        fixture.expectClosedConsumerControlResponse(archive);
        List<byte[]> events = new ArrayList<>();
        try (CatalogSubscription consumer = new CatalogSubscription(archive, LOG, SequencerService.OUTPUT_STREAM,
            from, "aeron:ipc?term-length=1m", stream)) {
            fixture.await(() -> consumer.sequence() == through, () -> consumer.poll((buffer, offset, length, event) -> {
                byte[] copy = new byte[length]; buffer.getBytes(offset, copy); events.add(copy);
            }, 256), "contiguous recorded output");
        }
        return events;
    }
    private void initialize(ClusterFixture fixture) {
        UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        int length = wire.encodeInitialize(init, 0, LOG);
        assertResult(fixture.exchange(init, length), AdmissionStatus.INITIALIZED);
    }
    private void assertResult(byte[] bytes, AdmissionStatus status) {
        assertThat(wire.validResult(new UnsafeBuffer(bytes), 0, bytes.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(status);
    }
    private static byte[] canonical(byte[] reply) { return Arrays.copyOfRange(reply, Wire.RESULT_OVERHEAD, reply.length); }
    private Command proposal(long producer, long sequence, long value, int payloadLength) {
        UnsafeBuffer payload = new UnsafeBuffer(new byte[payloadLength]);
        payload.putShort(0, (short)8, Wire.ORDER); payload.putShort(2, (short)1, Wire.ORDER);
        payload.putShort(4, (short)301, Wire.ORDER); payload.putLong(8, value, Wire.ORDER);
        for (int i = 16; i < payloadLength; i++) payload.putByte(i, (byte)(value + i));
        UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payloadLength]);
        return new Command(command, wire.encodeProposal(command, 0, LOG, producer, sequence,
            7, 11, 13, value * 10, value * 10 + 1, value * 10 + 2, payload, 0, payloadLength));
    }
    private record Command(UnsafeBuffer buffer, int length) {}

    private static final class ObservedService implements ClusteredService {
        private final SequencerService delegate = new SequencerService();
        final AtomicInteger selfOffers = new AtomicInteger();
        volatile boolean restored;
        volatile int replayed;
        volatile boolean starting = true;
        @Override public void onStart(Cluster cluster, Image snapshot) {
            restored = snapshot != null;
            Cluster observed = (Cluster)Proxy.newProxyInstance(Cluster.class.getClassLoader(), new Class[]{Cluster.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("offer")) { selfOffers.incrementAndGet(); throw new AssertionError("second Cluster commit"); }
                    try { return method.invoke(cluster, arguments); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
            delegate.onStart(observed, snapshot);
        }
        @Override public void onSessionMessage(ClientSession session, long timestamp, DirectBuffer buffer, int offset, int length, Header header) {
            replayed++;
            delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
        }
        @Override public void onRoleChange(Cluster.Role role) { starting = false; delegate.onRoleChange(role); }
        @Override public void onTakeSnapshot(ExclusivePublication publication) { delegate.onTakeSnapshot(publication); }
        @Override public void onTerminate(Cluster cluster) { delegate.onTerminate(cluster); }
        @Override public void onSessionOpen(ClientSession session, long timestamp) { delegate.onSessionOpen(session, timestamp); }
        @Override public void onSessionClose(ClientSession session, long timestamp, CloseReason reason) { delegate.onSessionClose(session, timestamp, reason); }
        @Override public void onTimerEvent(long correlation, long timestamp) { delegate.onTimerEvent(correlation, timestamp); }
    }
}
