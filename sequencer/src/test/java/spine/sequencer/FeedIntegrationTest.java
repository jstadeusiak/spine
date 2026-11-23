package spine.sequencer;

import com.sun.management.ThreadMXBean;
import io.aeron.archive.client.AeronArchive;
import spine.client.CatalogSubscription;
import spine.feeds.CapturedIngress;
import spine.feeds.NodeFileCollector;
import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual capture, admission and recorded-output recovery; source B supplies a retained public frame fixture. */
class FeedIntegrationTest {
    private static volatile Object escaped;
    @TempDir Path directory;
    private final Wire wire = new Wire();
    private final CaptureCodec codec = new CaptureCodec();

    @Test @Timeout(180)
    void warmedRealNodeFileCaptureAndAdmissionOwnerAllocatesZero() throws Exception {
        final java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assertThat(management).isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean)management;
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        if (!bean.isThreadAllocatedMemoryEnabled()) { bean.setThreadAllocatedMemoryEnabled(true); }
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        // Each 4 KiB source read supplies eight frames; warm the file-read path as well as admission.
        final int warm = 100_000, prime = 1024, small = 512, large = 1024;
        final byte[] sourceBytes = new byte[(warm + prime + small + large + 2) * 512];
        Arrays.fill(sourceBytes, (byte)'a');
        for (int offset = 511; offset < sourceBytes.length; offset += 512) { sourceBytes[offset] = '\n'; }
        final Path source = directory.resolve("allocation-segment");
        Files.write(source, sourceBytes);
        try (ClusterFixture fixture = new ClusterFixture(directory.resolve("allocation-cluster"), new SequencerService(), true)) {
            fixture.connect();
            final UnsafeBuffer initialize = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            fixture.exchange(initialize, wire.encodeInitialize(initialize, 0, 42));
            try (AeronArchive archive = fixture.connectArchive(8401)) {
                final NodeFileCollector collector = new NodeFileCollector(source, System.nanoTime());
                try (CapturedIngress ingress = ingress(fixture, archive, 10, 7, 6401, 7401, collector)) {
                    admitDefinition(fixture, ingress, collector.session(), "node", 1, 1);
                    collector.attach(ingress, 1, 1);
                    try (NodeOwner owner = new NodeOwner(collector, ingress)) {
                        owner.start();
                        awaitOwner(fixture, owner, () -> owner.acknowledged == NodeOwner.PAUSE);
                        final long controlBefore = allocated(bean, owner);
                        owner.request = NodeOwner.ALLOCATE;
                        awaitOwner(fixture, owner, () -> owner.acknowledged == NodeOwner.ALLOCATE);
                        owner.request = NodeOwner.PAUSE;
                        awaitOwner(fixture, owner, () -> owner.acknowledged == NodeOwner.PAUSE);
                        assertThat(allocated(bean, owner) - controlBefore).as("escaping owner allocation").isGreaterThanOrEqualTo(4096);

                        for (int admitted = 10_000; admitted <= warm; admitted += 10_000) {
                            driveOwner(fixture, owner, admitted + 1);
                        }
                        window(bean, fixture, owner, warm + prime + 1);
                        final long smallBytes = window(bean, fixture, owner, warm + prime + small + 1);
                        final long largeBytes = window(bean, fixture, owner, warm + prime + small + large + 1);
                        assertThat(smallBytes).as("512 real node-file/capture/admission transitions").isZero();
                        assertThat(largeBytes).as("1024 real node-file/capture/admission transitions").isZero();
                        assertThat(owner.sequence).isEqualTo(warm + prime + small + large + 1);
                        assertThat(owner.calls).as("actual bounded Agent calls").isGreaterThan(warm + prime + small + large);
                    }
                } finally { collector.onClose(); }
            }
            fixture.assertHealthy();
        }
    }

    @Test @Timeout(120)
    void fragmentedNodeFramesTwoSourcesExactRestartAndCorruptPrefixFailure() throws IOException {
        final Path source = directory.resolve("node-segment");
        final byte[] maximum = maximumFrame();
        final byte[] second = "{\"block_number\":7,\"events\":[]}".getBytes(StandardCharsets.UTF_8);
        final byte[] third = "{\"block_number\":7,\"events\":[{\"oid\":2}]}".getBytes(StandardCharsets.UTF_8);
        Files.write(source, maximum);
        Files.write(source, new byte[]{'\n'}, StandardOpenOption.APPEND);
        Files.write(source, second, StandardOpenOption.APPEND);
        Files.write(source, new byte[]{'\n'}, StandardOpenOption.APPEND);

        try (ClusterFixture fixture = new ClusterFixture(directory.resolve("cluster"), new SequencerService(), true)) {
            fixture.connect();
            final UnsafeBuffer initialize = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            final byte[] initialized = fixture.exchange(initialize, wire.encodeInitialize(initialize, 0, 42));
            assertThat(wire.validResult(new UnsafeBuffer(initialized), 0, initialized.length)).isTrue();
            assertThat(wire.result.status()).isEqualTo(AdmissionStatus.INITIALIZED);

            try (AeronArchive archive = fixture.connectArchive(8301)) {
                fixture.expectClosedConsumerControlResponse(archive);
                final NodeFileCollector initial = new NodeFileCollector(source, System.nanoTime());
                try (CapturedIngress node = ingress(fixture, archive, 10, 7, 6301, 7301, initial);
                     CapturedIngress publicFeed = ingress(fixture, archive, 11, 8, 6302, 7302, null)) {
                    admitDefinition(fixture, node, initial.session(), "node", 1, 1);
                    admitDefinition(fixture, publicFeed, "public-fixture-session", "public-ws", 1, 1);
                    assertThat(collect(fixture, archive, 2, 9301)).hasSize(2);
                    initial.attach(node, 1, 1);
                    fixture.await(() -> node.idle() && node.acknowledgedSequence() == 4,
                        () -> work(initial), "both complete node frames admitted");

                    final byte[] publicRaw = "{\"channel\":\"l2Book\",\"data\":{\"coin\":\"BTC\",\"time\":7,\"levels\":[[],[]]}}"
                        .getBytes(StandardCharsets.UTF_8);
                    final UnsafeBuffer observed = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
                    final int observedLength = codec.encodeObservation(observed, 0, 1, 0, CaptureCodec.UNKNOWN_NATIVE_ID,
                        null, 0, 0, new UnsafeBuffer(publicRaw), 0, publicRaw.length);
                    assertThat(publicFeed.submit(1, 1, 201, 202, 203, observed, 0, observedLength)).isTrue();
                    fixture.await(publicFeed::idle, publicFeed::poll, "public frame captured and admitted");
                    final List<byte[]> before = collect(fixture, archive, 5, 9302);
                    assertDefinition(before.get(0), 7, 1, 1, initial.session());
                    assertDefinition(before.get(1), 8, 1, 1, "public-fixture-session");
                    assertObservation(before.get(2), 7, 1, 1, 1, maximum);
                    assertObservation(before.get(3), 7, 1, 1, 2, second);
                    assertObservation(before.get(4), 8, 1, 1, 1, publicRaw);
                    assertThat(wire.validEvent(new UnsafeBuffer(before.get(4)), 0, before.get(4).length)).isTrue();
                    assertThat(wire.event.receiveTimeNanos()).isEqualTo(201);
                    assertThat(wire.event.completeTimeNanos()).isEqualTo(202);
                    assertThat(wire.event.readyTimeNanos()).isEqualTo(203);
                } finally { initial.onClose(); }

                final List<byte[]> beforeRestart = collect(fixture, archive, 5, 9303);
                final NodeFileCollector restarted = new NodeFileCollector(source, System.nanoTime());
                try (CapturedIngress node = ingress(fixture, archive, 10, 7, 6301, 7303, restarted)) {
                    fixture.await(node::idle, node::poll, "latest exact captured proposal duplicate receipt");
                    assertThat(node.acknowledgedSequence()).isEqualTo(4);
                    assertThat(collect(fixture, archive, 5, 9304)).containsExactlyElementsOf(beforeRestart);
                    admitDefinition(fixture, node, restarted.session(), "node", 2, 2);
                    restarted.attach(node, 2, 2);
                    Files.write(source, third, StandardOpenOption.APPEND);
                    Files.write(source, new byte[]{'\n'}, StandardOpenOption.APPEND);
                    fixture.await(() -> node.idle() && node.acknowledgedSequence() == 7,
                        () -> work(restarted), "new origin definition and next raw frame admitted");
                    final List<byte[]> continued = collect(fixture, archive, 7, 9305);
                    assertThat(continued.subList(0, 5)).containsExactlyElementsOf(beforeRestart);
                    assertDefinition(continued.get(5), 7, 2, 2, restarted.session());
                    assertObservation(continued.get(6), 7, 2, 2, 3, third);
                } finally { restarted.onClose(); }

                try (FileChannel changed = FileChannel.open(source, StandardOpenOption.WRITE)) {
                    assertThat(changed.write(ByteBuffer.wrap(new byte[]{'!'}), 0)).isEqualTo(1);
                }
                final NodeFileCollector corrupted = new NodeFileCollector(source, System.nanoTime());
                try {
                    assertThatThrownBy(() -> {
                        try (CapturedIngress ignored = ingress(fixture, archive, 10, 7, 6301, 7304, corrupted)) {
                            throw new AssertionError("corrupted source prefix connected");
                        }
                    }).isInstanceOf(UncheckedIOException.class).hasMessageContaining("differs");
                    assertThat(collect(fixture, archive, 7, 9306)).hasSize(7);
                } finally { corrupted.onClose(); }
            }
            fixture.assertHealthy();
        }
    }

    private CapturedIngress ingress(final ClusterFixture fixture, final AeronArchive archive, final long producer,
                                    final int source, final int capture, final int replay, final NodeFileCollector collector) {
        return new CapturedIngress(archive, fixture.producerContext(), 42, producer, source, capture, replay,
            collector == null ? null : collector::recovered);
    }
    private void admitDefinition(final ClusterFixture fixture, final CapturedIngress ingress, final String session,
                                 final String provider, final int configuration, final int clock) {
        final byte[][] labels = Arrays.stream(new String[]{provider, "BTC", session, "fixture", "{}", "relative"})
            .map(value -> value.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
        final UnsafeBuffer definition = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final int length = codec.encodeDefinition(definition, 0, -1, labels);
        assertThat(ingress.submit(configuration, clock, -1, -1, -1, definition, 0, length)).isTrue();
        fixture.await(ingress::idle, ingress::poll, "source definition admitted before observations");
    }
    private List<byte[]> collect(final ClusterFixture fixture, final AeronArchive archive, final long through, final int stream) {
        final List<byte[]> events = new ArrayList<>();
        try (CatalogSubscription subscription = new CatalogSubscription(archive, 42, SequencerService.OUTPUT_STREAM,
            0, "aeron:ipc?term-length=1m", stream)) {
            fixture.await(() -> subscription.sequence() == through, () -> {
                if (subscription.requiresRefresh()) { subscription.refresh(); }
                subscription.poll((buffer, offset, length, event) -> {
                    final byte[] copy = new byte[length];
                    buffer.getBytes(offset, copy);
                    events.add(copy);
                }, 256);
            }, "complete contiguous captured output");
        }
        return events;
    }
    private void assertDefinition(final byte[] bytes, final int source, final int config, final int clock, final String session) {
        final UnsafeBuffer event = new UnsafeBuffer(bytes);
        assertMetadata(event, source, config, clock);
        assertThat(codec.validDefinition(event, Wire.EVENT_OVERHEAD, bytes.length - Wire.EVENT_OVERHEAD)).isTrue();
        final byte[] recorded = new byte[codec.labelLength(2)];
        event.getBytes(codec.labelOffset(2), recorded);
        assertThat(recorded).containsExactly(session.getBytes(StandardCharsets.UTF_8));
    }
    private void assertObservation(final byte[] bytes, final int source, final int config, final int clock,
                                   final long frame, final byte[] raw) {
        final UnsafeBuffer event = new UnsafeBuffer(bytes);
        assertMetadata(event, source, config, clock);
        assertThat(wire.event.receiveTimeNanos()).isGreaterThanOrEqualTo(0);
        assertThat(wire.event.completeTimeNanos()).isGreaterThanOrEqualTo(wire.event.receiveTimeNanos());
        assertThat(wire.event.readyTimeNanos()).isGreaterThanOrEqualTo(wire.event.completeTimeNanos());
        assertThat(codec.validObservation(event, Wire.EVENT_OVERHEAD, bytes.length - Wire.EVENT_OVERHEAD)).isTrue();
        assertThat(codec.observation.rawFrameSequence()).isEqualTo(frame);
        assertThat(codec.observation.subeventIndex()).isZero();
        assertThat(codec.observation.nativeNumericId()).isEqualTo(CaptureCodec.UNKNOWN_NATIVE_ID);
        assertThat(codec.nativeLength).isZero();
        final byte[] recorded = new byte[codec.rawLength];
        event.getBytes(codec.rawOffset, recorded);
        assertThat(recorded).containsExactly(raw);
    }
    private void assertMetadata(final UnsafeBuffer event, final int source, final int config, final int clock) {
        assertThat(wire.validEvent(event, 0, event.capacity())).isTrue();
        assertThat(wire.event.sourceId()).isEqualTo(source);
        assertThat(wire.event.sourceConfigVersion()).isEqualTo(config);
        assertThat(wire.event.clockId()).isEqualTo(clock);
    }
    private static void work(final NodeFileCollector collector) {
        try { collector.doWork(); }
        catch (final IOException failure) { throw new UncheckedIOException(failure); }
    }
    private static byte[] maximumFrame() {
        final byte[] raw = new byte[Wire.MAX_PAYLOAD_BYTES - CaptureCodec.OBSERVATION_OVERHEAD];
        Arrays.fill(raw, (byte)'a');
        final byte[] prefix = "{\"block_number\":7,\"events\":[],\"padding\":\"".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(prefix, 0, raw, 0, prefix.length);
        raw[raw.length - 2] = '"';
        raw[raw.length - 1] = '}';
        return raw;
    }

    private static long window(final ThreadMXBean bean, final ClusterFixture fixture, final NodeOwner owner,
                               final long target) {
        final long before = allocated(bean, owner);
        driveOwner(fixture, owner, target);
        final long after = allocated(bean, owner);
        if (after < before) { throw new AssertionError("owner allocation accounting decreased"); }
        return after - before;
    }
    private static long allocated(final ThreadMXBean bean, final NodeOwner owner) {
        final long bytes = bean.getThreadAllocatedBytes(owner.threadId());
        if (bytes < 0) { throw new AssertionError("node owner allocation accounting unavailable"); }
        return bytes;
    }
    private static void driveOwner(final ClusterFixture fixture, final NodeOwner owner, final long target) {
        owner.target = target;
        owner.request = NodeOwner.RUN;
        awaitOwner(fixture, owner, () -> owner.sequence == target && owner.acknowledged == NodeOwner.PAUSE);
        owner.request = NodeOwner.PAUSE;
        awaitOwner(fixture, owner, () -> owner.acknowledged == NodeOwner.PAUSE);
    }
    private static void awaitOwner(final ClusterFixture fixture, final NodeOwner owner, final BooleanSupplier complete) {
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (!complete.getAsBoolean()) {
            if (owner.failure != null) { throw new AssertionError("node owner failed", owner.failure); }
            fixture.assertHealthy();
            fixture.pollAdmin();
            if (System.nanoTime() - deadline >= 0) { throw new AssertionError("node owner barrier timeout"); }
            LockSupport.parkNanos(10_000);
        }
        if (owner.failure != null) { throw new AssertionError("node owner failed", owner.failure); }
    }
    /** Only this thread invokes the Agent. Parent allocations and fixture checks cannot enter its measurement. */
    private static final class NodeOwner extends Thread implements AutoCloseable {
        static final int PAUSE = 0, RUN = 1, ALLOCATE = 2, STOP = 3;
        final NodeFileCollector collector;
        final CapturedIngress ingress;
        volatile int request = PAUSE, acknowledged = -1;
        volatile long target, sequence, calls;
        volatile Throwable failure;

        NodeOwner(final NodeFileCollector collector, final CapturedIngress ingress) {
            super("node-file-allocation-owner");
            setDaemon(true);
            this.collector = collector;
            this.ingress = ingress;
            sequence = ingress.acknowledgedSequence();
        }
        @Override public void run() {
            try {
                while (true) {
                    final int phase = request;
                    if (phase == STOP) { return; }
                    if (phase == RUN && sequence < target) {
                        final int work = collector.doWork();
                        sequence = ingress.acknowledgedSequence();
                        calls++;
                        acknowledged = RUN;
                        if (work == 0) { LockSupport.parkNanos(1_000); }
                    } else if (phase == ALLOCATE) {
                        if (acknowledged != ALLOCATE) { escaped = new byte[4096]; acknowledged = ALLOCATE; }
                        Thread.onSpinWait();
                    } else { acknowledged = PAUSE; Thread.onSpinWait(); }
                }
            } catch (final Throwable error) { failure = error; }
        }
        @Override public void close() throws InterruptedException {
            request = STOP;
            join(10_000);
            if (isAlive()) { interrupt(); throw new AssertionError("node owner did not stop"); }
        }
    }
}
