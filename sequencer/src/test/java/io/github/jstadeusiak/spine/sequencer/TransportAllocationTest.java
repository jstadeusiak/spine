package io.github.jstadeusiak.spine.sequencer;

import com.sun.management.ThreadMXBean;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.client.CatalogSubscription;
import io.github.jstadeusiak.spine.client.EventHandler;
import io.github.jstadeusiak.spine.client.ReliableProducer;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual transport owners; the observing test thread never polls or runs assertions on either owner. */
class TransportAllocationTest {
    @TempDir Path directory;
    private static final int SMALL_WARMUP = 32_768;
    private static final int SMALL_PAYLOAD = 16;
    private static volatile Object escaped;

    @Test @Timeout(180)
    void warmedProducerServiceAndArchiveFollowerAllocateZeroAtMaximumSize() throws Exception {
        final ThreadMXBean bean = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        if (!bean.isThreadAllocatedMemoryEnabled()) { bean.setThreadAllocatedMemoryEnabled(true); }
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final MeasuredService service = new MeasuredService(bean);
        try (ClusterFixture fixture = new ClusterFixture(directory, service, true)) {
            fixture.connect();
            final Wire wire = new Wire();
            final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            fixture.exchange(init, wire.encodeInitialize(init, 0, 42));
            try (ReliableProducer producer = new ReliableProducer(42, 10, 1);
                 AeronArchive archive = fixture.connectArchive(8801);
                 CatalogSubscription consumer = new CatalogSubscription(archive, 42, SequencerService.OUTPUT_STREAM,
                     0, "aeron:ipc?term-length=1m", 9801);
                 ProducerOwner sender = new ProducerOwner(producer);
                 ConsumerOwner receiver = new ConsumerOwner(consumer)) {
                fixture.expectClosedConsumerControlResponse(archive);
                producer.connect(fixture.producerContext());
                sender.start();
                receiver.start();
                pause(fixture, sender, receiver);
                assertThat(bytes(bean, sender)).isNotNegative();
                assertThat(bytes(bean, receiver)).isNotNegative();
                negativeControl(bean, fixture, sender);
                negativeControl(bean, fixture, receiver);

                sender.request = Owner.RUN;
                receiver.request = Owner.RUN;
                await(fixture, sender, receiver, () -> sender.idlePolls >= 10_000 && receiver.idlePolls >= 10_000);
                pause(fixture, sender, receiver);
                service.measure = true;
                // Link/compile the same decoder, hash and callback methods using real traffic, without gigabytes of fixtures.
                drive(fixture, service, sender, receiver, SMALL_WARMUP);
                sender.payloadLength = Wire.MAX_PAYLOAD_BYTES;
                drive(fixture, service, sender, receiver, SMALL_WARMUP + 4096);
                drive(fixture, service, sender, receiver, SMALL_WARMUP + 4352);

                // A collector can keep its connected owner live with a pending maximum-size capture.
                sender.offerPending = false;
                sender.target = sender.sequence + 1;
                gatedPolls(fixture, sender, receiver);
                final long gatedBefore = bytes(bean, sender);
                gatedPolls(fixture, sender, receiver);
                assertThat(bytes(bean, sender) - gatedBefore).as("10,000 gated producer polls").isZero();
                assertThat(service.completedSequence).as("gated proposal never reaches the service").isLessThan(sender.target);
                assertThat(producer.pendingLength()).isEqualTo(Wire.MAX_PROPOSAL_BYTES);
                sender.offerPending = true;

                final long[] small = window(bean, fixture, service, sender, receiver, SMALL_WARMUP + 4864);
                final long[] large = window(bean, fixture, service, sender, receiver, SMALL_WARMUP + 5888);
                service.measure = false;
                assertThat(small[0]).as("512 producer-owner observations").isZero();
                assertThat(large[0]).as("1024 producer-owner observations").isZero();
                assertThat(small[1]).as("512 Archive-follow consumer observations").isZero();
                assertThat(large[1]).as("1024 Archive-follow consumer observations").isZero();
                assertThat(small[2]).as("512 core/publication/receipt observations").isZero();
                assertThat(large[2]).as("1024 core/publication/receipt observations").isZero();
                assertThat(small[3]).isGreaterThanOrEqualTo(512);
                assertThat(large[3]).isGreaterThanOrEqualTo(1024);
                assertThat(sender.sequence).isEqualTo(SMALL_WARMUP + 5888);
                assertThat(receiver.sequence).isEqualTo(SMALL_WARMUP + 5888);
                assertThat(consumer.sequence()).isEqualTo(SMALL_WARMUP + 5888);
            }
            fixture.assertHealthy();
        }
    }

    private static void gatedPolls(final ClusterFixture fixture, final ProducerOwner sender,
                                   final ConsumerOwner receiver) {
        final long targetPolls = sender.idlePolls + 10_000;
        sender.request = Owner.RUN;
        await(fixture, sender, receiver, () -> sender.idlePolls >= targetPolls);
        pause(fixture, sender, receiver);
    }

    /** Paused owners publish an acknowledgement only after any preceding poll and callback have returned. */
    private static void pause(final ClusterFixture fixture, final Owner sender, final Owner receiver) {
        sender.request = Owner.PAUSE;
        receiver.request = Owner.PAUSE;
        await(fixture, sender, receiver, () -> sender.acknowledged == Owner.PAUSE && receiver.acknowledged == Owner.PAUSE);
    }

    private static void drive(final ClusterFixture fixture, final MeasuredService service,
                              final ProducerOwner sender, final ConsumerOwner receiver, final long target) {
        sender.target = target;
        sender.request = Owner.RUN;
        receiver.request = Owner.RUN;
        await(fixture, sender, receiver, () -> sender.sequence == target && receiver.sequence == target
            && service.completedSequence >= target);
        pause(fixture, sender, receiver);
    }

    private static long[] window(final ThreadMXBean bean, final ClusterFixture fixture, final MeasuredService service,
                                 final ProducerOwner sender, final ConsumerOwner receiver, final long target) {
        final long producerBefore = bytes(bean, sender);
        final long consumerBefore = bytes(bean, receiver);
        final long serviceBefore = service.bytes;
        final long callsBefore = service.calls;
        drive(fixture, service, sender, receiver, target);
        return new long[] {bytes(bean, sender) - producerBefore, bytes(bean, receiver) - consumerBefore,
            service.bytes - serviceBefore, service.calls - callsBefore};
    }

    private static void negativeControl(final ThreadMXBean bean, final ClusterFixture fixture, final Owner owner) {
        final long before = bytes(bean, owner);
        owner.request = Owner.ALLOCATE;
        await(fixture, owner, owner, () -> owner.acknowledged == Owner.ALLOCATE);
        owner.request = Owner.PAUSE;
        await(fixture, owner, owner, () -> owner.acknowledged == Owner.PAUSE);
        assertThat(bytes(bean, owner) - before).as("escaping allocation on measured owner").isGreaterThanOrEqualTo(4096);
    }

    private static long bytes(final ThreadMXBean bean, final Owner owner) {
        final long value = bean.getThreadAllocatedBytes(owner.threadId());
        if (value < 0) { throw new AssertionError("owner allocation accounting unavailable"); }
        return value;
    }

    private static void await(final ClusterFixture fixture, final Owner sender, final Owner receiver,
                              final BooleanSupplier done) {
        final long deadline = System.nanoTime() + 30_000_000_000L;
        while (!done.getAsBoolean()) {
            sender.checkFailure();
            receiver.checkFailure();
            fixture.pollAdmin();
            fixture.assertHealthy();
            if (System.nanoTime() - deadline >= 0) { throw new AssertionError("transport owner barrier timeout"); }
            LockSupport.parkNanos(10_000);
        }
        sender.checkFailure();
        receiver.checkFailure();
    }

    /** Test-only owner coordination. Parent writes requests; owner writes acknowledgements and progress. */
    private abstract static class Owner extends Thread implements AutoCloseable {
        static final int PAUSE = 0, RUN = 1, ALLOCATE = 2, STOP = 3;
        volatile int request = PAUSE;
        volatile int acknowledged = -1;
        volatile long idlePolls;
        volatile Throwable failure;

        Owner(final String name) { super(name); setDaemon(true); }

        @Override public final void run() {
            try {
                while (true) {
                    final int phase = request;
                    if (phase == STOP) { return; }
                    if (phase == RUN) {
                        final int work = poll();
                        acknowledged = RUN;
                        if (work == 0) { idlePolls++; LockSupport.parkNanos(1_000); }
                    } else if (phase == ALLOCATE) {
                        if (acknowledged != ALLOCATE) { escaped = new byte[4096]; acknowledged = ALLOCATE; }
                        Thread.onSpinWait();
                    } else {
                        acknowledged = PAUSE;
                        Thread.onSpinWait();
                    }
                }
            } catch (final Throwable error) { failure = error; }
        }

        abstract int poll();

        final void checkFailure() {
            if (failure != null) { throw new AssertionError("transport owner failed: " + getName(), failure); }
            if (getState() == State.TERMINATED && request != STOP) { throw new AssertionError("owner terminated"); }
        }

        @Override public final void close() throws InterruptedException {
            request = STOP;
            join(10_000);
            if (isAlive()) { interrupt(); throw new AssertionError("transport owner did not stop"); }
        }
    }

    private static final class ProducerOwner extends Owner {
        final ReliableProducer producer;
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        volatile long target;
        volatile long sequence;
        volatile int payloadLength = SMALL_PAYLOAD;
        volatile boolean offerPending = true;

        ProducerOwner(final ReliableProducer producer) {
            super("spine-allocation-producer");
            this.producer = producer;
            payload.putShort(0, (short)8, Wire.ORDER);
            payload.putShort(2, (short)1, Wire.ORDER);
            payload.putShort(4, (short)301, Wire.ORDER);
        }

        @Override int poll() {
            if (producer.pendingLength() == 0 && sequence < target) {
                payload.putLong(8, producer.nextSequence(), Wire.ORDER);
                if (!producer.submit(1, 1, 1, -1, -1, -1, payload, 0, payloadLength)) {
                    throw new AssertionError("producer slot refused");
                }
            }
            final int work = producer.poll(offerPending);
            if (producer.failureReason() != null) { throw new AssertionError("producer failed", producer.failureReason()); }
            sequence = producer.acknowledgedSequence();
            return work;
        }
    }

    private static final class ConsumerOwner extends Owner {
        final CatalogSubscription consumer;
        volatile long sequence;
        final EventHandler handler = (buffer, offset, length, event) -> {
            final int expectedLength = Wire.EVENT_OVERHEAD
                + (event.sequence() <= SMALL_WARMUP ? SMALL_PAYLOAD : Wire.MAX_PAYLOAD_BYTES);
            if (event.sequence() != sequence + 1 || length != expectedLength
                || buffer.getLong(offset + Wire.EVENT_OVERHEAD + 8, Wire.ORDER) != event.sequence()) {
                throw new AssertionError("maximum-size event identity or bytes differ");
            }
            sequence = event.sequence();
        };

        ConsumerOwner(final CatalogSubscription consumer) { super("spine-allocation-consumer"); this.consumer = consumer; }
        @Override int poll() { return consumer.poll(handler, 256); }
    }

    private static final class MeasuredService implements ClusteredService {
        final SequencerService delegate = new SequencerService();
        final ThreadMXBean bean;
        final Wire commands = new Wire();
        volatile boolean measure;
        volatile long bytes, calls, completedSequence;

        MeasuredService(final ThreadMXBean bean) { this.bean = bean; }
        @Override public void onStart(final Cluster cluster, final Image snapshot) { delegate.onStart(cluster, snapshot); }
        @Override public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                              final int offset, final int length, final Header header) {
            final long owner = Thread.currentThread().threadId();
            final long before = measure ? bean.getThreadAllocatedBytes(owner) : 0;
            delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
            if (measure) { bytes += bean.getThreadAllocatedBytes(owner) - before; calls++; }
            if (commands.validProposal(buffer, offset, length)) { completedSequence = commands.proposal.producerSequence(); }
        }
        @Override public void onTakeSnapshot(final ExclusivePublication publication) { delegate.onTakeSnapshot(publication); }
        @Override public void onTerminate(final Cluster cluster) { delegate.onTerminate(cluster); }
        @Override public void onRoleChange(final Cluster.Role role) { delegate.onRoleChange(role); }
        @Override public void onSessionOpen(final ClientSession session, final long timestamp) { delegate.onSessionOpen(session, timestamp); }
        @Override public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason reason) {
            delegate.onSessionClose(session, timestamp, reason);
        }
        @Override public void onTimerEvent(final long correlation, final long timestamp) { delegate.onTimerEvent(correlation, timestamp); }
    }
}
