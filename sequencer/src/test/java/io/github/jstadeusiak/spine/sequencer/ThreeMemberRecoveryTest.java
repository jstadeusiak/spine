package io.github.jstadeusiak.spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.RecordingLog;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import io.github.jstadeusiak.spine.client.CatalogSubscription;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class ThreeMemberRecoveryTest {
    private static final long LOG = 42;
    private static final String IPC = "aeron:ipc?term-length=1m";
    private static final String UDP = "aeron:udp?interface=127.0.0.1";
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);
    @TempDir Path directory;
    private final Wire wire = new Wire();

    @Test @Timeout(150)
    void leaderLossExactRetryAndOlderSnapshotTailProduceIdenticalMemberOutput() {
        try (Fixture fixture = new Fixture(directory)) {
            fixture.connect();
            final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            result(fixture.exchange(init, wire.encodeInitialize(init, 0, LOG)), AdmissionStatus.INITIALIZED);
            final List<byte[]> expected = new ArrayList<>();
            final Command first = proposal(1), second = proposal(2), third = proposal(3), fourth = proposal(4);
            final byte[] firstReply = fixture.exchange(first.buffer(), first.length());
            result(firstReply, AdmissionStatus.ACCEPTED);
            expected.add(canonical(firstReply));
            fixture.snapshot();
            final byte[] secondReply = fixture.exchange(second.buffer(), second.length());
            result(secondReply, AdmissionStatus.ACCEPTED);
            expected.add(canonical(secondReply));
            fixture.snapshot();
            fixture.discard = true;
            fixture.send(third.buffer(), third.length());
            fixture.await(() -> fixture.discarded != null && fixture.applied(3), () -> { }, "lost admitted receipt");
            result(fixture.discarded, AdmissionStatus.ACCEPTED);
            expected.add(canonical(fixture.discarded));
            final int failedLeader = fixture.leader();
            fixture.disconnect();
            fixture.expectLeaderLoss = true;
            fixture.stop(failedLeader);
            fixture.await(() -> fixture.leader() >= 0, () -> { }, "survivor election");
            fixture.connect();
            final byte[] retry = fixture.exchange(third.buffer(), third.length());
            result(retry, AdmissionStatus.DUPLICATE);
            assertThat(canonical(retry)).isEqualTo(expected.get(2));
            for (int id = 0; id < 3; id++) {
                if (id != failedLeader) { assertThat(fixture.collect(id, 3)).containsExactlyElementsOf(expected); }
            }
            fixture.healthy();
            assertThat(fixture.leaderLossWarnings).isBetween(1, 2);
            fixture.expectLeaderLoss = false;
            try (RecordingLog log = new RecordingLog(fixture.state(failedLeader).resolve("cluster").toFile(), false)) {
                final long latestPosition = log.getLatestSnapshot(0).logPosition;
                assertThat(log.invalidateLatestSnapshot()).isTrue();
                log.force(2);
                assertThat(log.getLatestSnapshot(0)).isNotNull();
                assertThat(log.getLatestSnapshot(0).logPosition).isLessThan(latestPosition);
            }
            fixture.launch(failedLeader, false);
            fixture.await(() -> fixture.members[failedLeader].service.restored && fixture.applied(3),
                () -> { }, "older snapshot and retained tail recovery");
            assertThat(fixture.members[failedLeader].service.seen & 12)
                .as("older snapshot replays proposals two and three").isEqualTo(12);
            final byte[] next = fixture.exchange(fourth.buffer(), fourth.length());
            result(next, AdmissionStatus.ACCEPTED);
            expected.add(canonical(next));
            fixture.await(() -> fixture.applied(4), () -> { }, "all members apply new tail");
            for (int id = 0; id < 3; id++) { assertThat(fixture.collect(id, 4)).containsExactlyElementsOf(expected); }
        }
    }

    private void result(final byte[] bytes, final AdmissionStatus expected) {
        assertThat(wire.validResult(new UnsafeBuffer(bytes), 0, bytes.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(expected);
    }

    private static byte[] canonical(final byte[] bytes) { return Arrays.copyOfRange(bytes, Wire.RESULT_OVERHEAD, bytes.length); }

    private Command proposal(final int sequence) {
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[4096]);
        payload.putShort(0, (short)8, Wire.ORDER);
        payload.putShort(2, (short)1, Wire.ORDER);
        payload.putShort(4, (short)301, Wire.ORDER);
        payload.putLong(8, sequence, Wire.ORDER);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payload.capacity()]);
        return new Command(buffer, wire.encodeProposal(buffer, 0, LOG, 1, sequence, 7, 11, 13,
            sequence * 10L, sequence * 10L + 1, sequence * 10L + 2, payload, 0, payload.capacity()));
    }

    private record Command(UnsafeBuffer buffer, int length) { }
    private record Member(ObservedService service, ClusteredMediaDriver driver, ClusteredServiceContainer container) { }

    /** Test-only local native quorum; persistent member archives, independent transient client driver. */
    private static final class Fixture implements AutoCloseable {
        private static final List<String> HOSTS = List.of("127.0.0.1", "127.0.0.1", "127.0.0.1");
        private final Path directory;
        private final int ports = availablePorts();
        private final Member[] members = new Member[3];
        private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        private final List<byte[]> replies = new ArrayList<>();
        private final MediaDriver clientDriver;
        private AeronCluster client;
        private long keepAlive;
        private boolean discard;
        private byte[] discarded;
        private boolean expectLeaderLoss;
        private int leaderLossWarnings;

        Fixture(final Path directory) {
            this.directory = directory;
            clientDriver = MediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(directory.resolve("client").toString())
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true).errorHandler(errors::add));
            try {
                for (int id = 0; id < 3; id++) { launch(id, true); }
                // A quorum can accept a client before the third follower has joined its log.
                await(() -> {
                    for (final Member member : members) {
                        final AtomicCounter election = member.driver.consensusModule().context().electionStateCounter();
                        if (election.isClosed() || election.get() != ElectionState.CLOSED.code()) { return false; }
                    }
                    return leader() >= 0;
                }, () -> { }, "all members join initial live log");
            }
            catch (final RuntimeException | Error failure) { close(); throw failure; }
        }

        Path state(final int id) { return directory.resolve("member-" + id); }

        void launch(final int id, final boolean clean) {
            final ObservedService service = new ObservedService();
            final ClusterConfig config = ClusterConfig.create(id, HOSTS, HOSTS, ports, service);
            service.delegate.configure(config.consensusModuleContext(), config.clusteredServiceContext());
            config.baseDir(state(id).toFile());
            config.aeronDirectoryName(directory.resolve("driver-" + id + "-" + System.nanoTime()).toString());
            config.errorHandler(errors::add);
            config.mediaDriverContext().threadingMode(ThreadingMode.SHARED).timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(5))
                .ipcTermBufferLength(1024 * 1024).ipcMtuLength(1024).publicationTermBufferLength(1024 * 1024)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true);
            config.archiveContext().deleteArchiveOnStart(clean).fileSyncLevel(0).segmentFileLength(1024 * 1024)
                .localControlChannel(IPC)
                .controlChannel(config.archiveContext().controlChannel() + "|interface=127.0.0.1")
                .replicationChannel(UDP + "|endpoint=127.0.0.1:0");
            config.archiveContext().archiveClientContext().controlResponseChannel(UDP + "|endpoint=127.0.0.1:0");
            config.aeronArchiveContext().controlRequestChannel(IPC).controlResponseChannel(IPC);
            config.consensusModuleContext().archiveContext(config.aeronArchiveContext().clone()).deleteDirOnStart(clean)
                .clusterClock(new NanosecondClusterClock()).ingressChannel(UDP).consensusChannel(UDP)
                .logChannel(UDP + "|term-length=1m").controlChannel(IPC).replayChannel(IPC)
                .replicationChannel(UDP + "|endpoint=127.0.0.1:0")
                .followerCatchupChannel(UDP).leaderArchiveControlChannel(UDP)
                .snapshotChannel("aeron:ipc?term-length=32m").terminationHook(() -> { });
            config.clusteredServiceContext().archiveContext(config.aeronArchiveContext().clone())
                .controlChannel(IPC).replayChannel(IPC).snapshotChannel("aeron:ipc?term-length=32m").terminationHook(() -> { });
            final ClusteredMediaDriver driver = ClusteredMediaDriver.launch(config.mediaDriverContext(),
                config.archiveContext(), config.consensusModuleContext());
            try {
                members[id] = new Member(service, driver, ClusteredServiceContainer.launch(config.clusteredServiceContext()));
            } catch (final RuntimeException | Error failure) { driver.close(); throw failure; }
        }

        int leader() {
            for (int id = 0; id < 3; id++) {
                if (members[id] != null && members[id].service.role == Cluster.Role.LEADER) { return id; }
            }
            return -1;
        }

        boolean applied(final int sequence) {
            for (final Member member : members) { if (member != null && member.service.last < sequence) { return false; } }
            return true;
        }

        void connect() {
            client = AeronCluster.connect(new AeronCluster.Context().aeronDirectoryName(clientDriver.aeronDirectoryName())
                .ingressChannel(UDP).egressChannel(UDP + "|endpoint=127.0.0.1:0")
                .ingressEndpoints(ClusterConfig.ingressEndpoints(HOSTS, ports, ClusterConfig.CLIENT_FACING_PORT_OFFSET))
                .messageTimeoutNs(TIMEOUT_NS).errorHandler(errors::add)
                .egressListener((session, time, buffer, offset, length, header) -> {
                    final byte[] copy = new byte[length];
                    buffer.getBytes(offset, copy);
                    if (discard) { discarded = copy; discard = false; }
                    else { replies.add(copy); }
                }));
        }

        void disconnect() { CloseHelper.close(client); client = null; }

        void send(final DirectBuffer buffer, final int length) {
            await(() -> client.offer(buffer, 0, length) > 0, () -> { }, "proposal publication");
        }

        byte[] exchange(final DirectBuffer buffer, final int length) {
            final int before = replies.size();
            send(buffer, length);
            final long[] retry = {System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250)};
            await(() -> replies.size() > before, () -> {
                if (System.nanoTime() >= retry[0] && client.offer(buffer, 0, length) > 0) {
                    retry[0] = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250);
                }
            }, "receipt");
            return replies.get(before);
        }

        void snapshot() {
            final long[] counts = Arrays.stream(members).mapToLong(m -> m.driver.consensusModule().context().snapshotCounter().get()).toArray();
            final Member leader = members[leader()];
            final AtomicCounter toggle = ClusterControl.findControlToggle(leader.driver.mediaDriver().context().countersManager(),
                leader.driver.consensusModule().context().clusterId());
            await(() -> ClusterControl.ToggleState.SNAPSHOT.toggle(toggle), () -> { }, "snapshot request");
            await(() -> {
                for (int id = 0; id < 3; id++) {
                    if (members[id].driver.consensusModule().context().snapshotCounter().get() <= counts[id]) { return false; }
                }
                return ClusterControl.ToggleState.get(toggle) == ClusterControl.ToggleState.NEUTRAL;
            }, () -> { }, "all native snapshots");
        }

        List<byte[]> collect(final int id, final long through) {
            final Member member = members[id];
            final List<byte[]> bytes = new ArrayList<>();
            try (AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(member.driver.mediaDriver().aeronDirectoryName())
                .controlRequestChannel(IPC).controlRequestStreamId(member.driver.archive().context().localControlStreamId())
                .controlResponseChannel(IPC).controlResponseStreamId(8601).messageTimeoutNs(TIMEOUT_NS));
                 CatalogSubscription consumer = new CatalogSubscription(archive, LOG, SequencerService.OUTPUT_STREAM, 0, IPC, 9601)) {
                await(() -> consumer.sequence() == through, () -> consumer.poll((buffer, offset, length, event) -> {
                    final byte[] copy = new byte[length]; buffer.getBytes(offset, copy); bytes.add(copy);
                }, 100), "member recorded output");
            }
            return bytes;
        }

        void await(final BooleanSupplier done, final Runnable pump, final String description) {
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (!done.getAsBoolean()) {
                healthy();
                pump.run();
                if (client != null) {
                    client.pollEgress();
                    if (System.nanoTime() - keepAlive >= TimeUnit.MILLISECONDS.toNanos(500)) {
                        client.sendKeepAlive(); keepAlive = System.nanoTime();
                    }
                }
                if (System.nanoTime() >= deadline) { throw new IllegalStateException("timed out awaiting " + description); }
                LockSupport.parkNanos(100_000);
            }
            healthy();
        }

        void healthy() {
            for (Throwable failure; (failure = errors.poll()) != null; ) {
                // Aeron can observe the deliberately closed leader's log before its heartbeat expires.
                if (expectLeaderLoss && leaderLossWarnings < 2 && failure instanceof ClusterEvent event
                    && event.category() == AeronException.Category.WARN
                    && (event.getMessage().equals("WARN - leader heartbeat timeout")
                        || event.getMessage().equals("WARN - log recording stopped: eos=true")
                        || event.getMessage().equals("WARN - log disconnected from leader: eos=true"))) {
                    leaderLossWarnings++;
                    continue;
                }
                // Archive can finish an already closed reader's private stop-replay acknowledgement after close.
                if (failure instanceof ArchiveEvent event && event.category() == AeronException.Category.WARN
                    && event.getMessage().startsWith("WARN - control response publication is not connected: ControlSession{")
                    && event.getMessage().contains("state=DONE,")) { continue; }
                throw new IllegalStateException("three-member fixture failed", failure);
            }
        }

        void stop(final int id) {
            final Member member = members[id]; members[id] = null;
            if (member != null) { CloseHelper.closeAll(member.driver.consensusModule(), member.container, member.driver); }
        }

        @Override public void close() {
            try { disconnect(); for (int id = 0; id < 3; id++) { stop(id); } }
            finally { clientDriver.close(); }
        }

        private static int availablePorts() {
            for (int attempt = 0; attempt < 100; attempt++) {
                final int base = ThreadLocalRandom.current().nextInt(20_000, 59_000);
                final List<DatagramSocket> sockets = new ArrayList<>();
                try {
                    for (int id = 0; id < 3; id++) {
                        for (int offset = 1; offset <= 5; offset++) {
                            final DatagramSocket socket = new DatagramSocket(null); sockets.add(socket);
                            socket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"),
                                ClusterConfig.calculatePort(id, base, offset)));
                        }
                    }
                    return base;
                } catch (final java.io.IOException unavailable) {
                    // Probe another complete member range.
                } finally { sockets.forEach(DatagramSocket::close); }
            }
            throw new IllegalStateException("no isolated cluster port range");
        }
    }

    private static final class ObservedService implements ClusteredService {
        private final SequencerService delegate = new SequencerService();
        private final Wire wire = new Wire();
        private volatile Cluster.Role role;
        private volatile long last;
        private volatile long seen;
        private volatile boolean restored;
        @Override public void onStart(final Cluster cluster, final Image snapshot) { delegate.onStart(cluster, snapshot); restored = snapshot != null; }
        @Override public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
            final int offset, final int length, final Header header) {
            delegate.onSessionMessage(session, timestamp, buffer, offset, length, header);
            if (wire.validProposal(buffer, offset, length)) { last = wire.proposal.producerSequence(); seen |= 1L << last; }
        }
        @Override public void onRoleChange(final Cluster.Role role) { this.role = role; delegate.onRoleChange(role); }
        @Override public void onTakeSnapshot(final ExclusivePublication publication) { delegate.onTakeSnapshot(publication); }
        @Override public void onTerminate(final Cluster cluster) { delegate.onTerminate(cluster); }
        @Override public void onSessionOpen(final ClientSession session, final long timestamp) { delegate.onSessionOpen(session, timestamp); }
        @Override public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason reason) { delegate.onSessionClose(session, timestamp, reason); }
        @Override public void onTimerEvent(final long correlationId, final long timestamp) { delegate.onTimerEvent(correlationId, timestamp); }
    }
}
