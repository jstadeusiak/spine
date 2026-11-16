package io.github.jstadeusiak.spine.sequencer;

import io.aeron.ChannelUri;
import io.aeron.archive.ControlSessionCounter;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.RecordingLog;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.AeronException;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;

/** An actual single-node Cluster, with test-owned storage, ports and bounded progress checks. */
public final class ClusterFixture implements AutoCloseable {
    private static final int TERM_LENGTH = 1024 * 1024;
    private static final String IPC = "aeron:ipc?term-length=1m";
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    private Predicate<Throwable> expectedClosedConsumerWarning = error -> false;
    private final AtomicBoolean consensusTerminated = new AtomicBoolean();
    private final AtomicBoolean serviceTerminated = new AtomicBoolean();
    private final List<byte[]> replies = new ArrayList<>();
    private final ClusteredMediaDriver driver;
    private final ClusteredServiceContainer container;
    private final String aeronDirectory;
    private final Path clusterDirectory;
    private final int archiveControlStreamId;
    private AeronCluster client;
    private long lastKeepAliveNanos;
    private boolean discardNextReply;
    private boolean closedStartupKeepAlive;
    private volatile boolean orderlyTermination;

    public ClusterFixture(final Path directory, final ClusteredService service, final boolean cleanStart) {
        this(directory, service, cleanStart, false);
    }

    public ClusterFixture(final Path directory, final ClusteredService service, final boolean cleanStart,
                   final boolean performance) {
        this(directory, service, cleanStart, performance, ConsensusModule.Configuration.sessionTimeoutNs());
    }

    public ClusterFixture(final Path directory, final ClusteredService service, final boolean cleanStart,
                   final boolean performance, final long sessionTimeoutNs) {
        this(directory, service, cleanStart, performance, sessionTimeoutNs, config -> { });
    }

    public ClusterFixture(final Path directory, final ClusteredService service, final boolean cleanStart,
                   final boolean performance, final long sessionTimeoutNs, final Consumer<ClusterConfig> configure) {
        aeronDirectory = directory.resolve("driver-" + UUID.randomUUID()).toString();
        final ClusterConfig config = ClusterConfig.create(0, List.of("127.0.0.1"), List.of("127.0.0.1"),
            availablePortBase(), service);
        // Forwarding test services use the version-one binding; direct services configure their declared identity.
        config.consensusModuleContext().appVersion(1).appVersionValidator((expected, actual) -> expected == actual);
        config.clusteredServiceContext().appVersion(1).appVersionValidator((expected, actual) -> expected == actual);
        if (service instanceof SequencerService sequencer) {
            sequencer.configure(config.consensusModuleContext(), config.clusteredServiceContext());
        }
        config.baseDir(directory.resolve("state").toFile());
        config.aeronDirectoryName(aeronDirectory);
        config.errorHandler(errors::add);
        config.mediaDriverContext()
            .threadingMode(performance ? ThreadingMode.DEDICATED : ThreadingMode.SHARED)
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(5))
            .ipcTermBufferLength(TERM_LENGTH)
            .ipcMtuLength(1024)
            .publicationTermBufferLength(TERM_LENGTH)
            .untetheredWindowLimitTimeoutNs(TimeUnit.MILLISECONDS.toNanos(20))
            .untetheredLingerTimeoutNs(TimeUnit.MILLISECONDS.toNanos(20))
            .untetheredRestingTimeoutNs(TimeUnit.MILLISECONDS.toNanos(50))
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true);
        config.archiveContext().deleteArchiveOnStart(cleanStart).fileSyncLevel(0).localControlChannel(IPC)
            .segmentFileLength(TERM_LENGTH);
        clusterDirectory = config.consensusModuleContext().clusterDir().toPath();
        archiveControlStreamId = config.archiveContext().localControlStreamId();
        config.aeronArchiveContext().controlRequestChannel(IPC).controlResponseChannel(IPC);
        config.consensusModuleContext()
            .archiveContext(config.aeronArchiveContext().clone())
            .sessionTimeoutNs(sessionTimeoutNs)
            .deleteDirOnStart(cleanStart)
            .clusterClock(new NanosecondClusterClock())
            .ingressChannel(IPC)
            .logChannel("aeron:udp?term-length=1m")
            .replayChannel(IPC)
            .controlChannel(IPC)
            .snapshotChannel("aeron:ipc?term-length=32m")
            .terminationHook(() -> consensusTerminated.set(true));
        config.clusteredServiceContext()
            .archiveContext(config.aeronArchiveContext().clone())
            .controlChannel(IPC)
            .replayChannel(IPC)
            .snapshotChannel("aeron:ipc?term-length=32m")
            .terminationHook(() -> {
                if (orderlyTermination) {
                    // Aeron 1.53.3 resends terminationPosition from consensus onClose after the service ack.
                    // Keep the service subscription alive until that cleanup completes.
                    final long deadline = System.nanoTime() + TIMEOUT_NS;
                    while (!config.consensusModuleContext().aeron().isClosed()) {
                        if (System.nanoTime() >= deadline) {
                            throw new IllegalStateException("consensus did not close after service termination ack");
                        }
                        LockSupport.parkNanos(100_000);
                    }
                }
                serviceTerminated.set(true);
            });
        if (performance) {
            config.mediaDriverContext().senderIdleStrategy(performanceIdleStrategy())
                .receiverIdleStrategy(performanceIdleStrategy()).conductorIdleStrategy(performanceIdleStrategy());
            config.consensusModuleContext().idleStrategySupplier(ClusterFixture::performanceIdleStrategy);
            config.clusteredServiceContext().idleStrategySupplier(ClusterFixture::performanceIdleStrategy);
            config.archiveContext().idleStrategySupplier(ClusterFixture::performanceIdleStrategy)
                .recorderIdleStrategySupplier(ClusterFixture::performanceIdleStrategy)
                .replayerIdleStrategySupplier(ClusterFixture::performanceIdleStrategy);
        }

        configure.accept(config);
        ClusteredMediaDriver openedDriver = null;
        ClusteredServiceContainer openedContainer = null;
        try {
            driver = openedDriver = ClusteredMediaDriver.launch(config.mediaDriverContext(),
                config.archiveContext(), config.consensusModuleContext());
            container = openedContainer = ClusteredServiceContainer.launch(config.clusteredServiceContext());
            assertHealthy();
        } catch (final RuntimeException | Error failure) {
            CloseHelper.quietCloseAll(openedContainer, openedDriver);
            throw failure;
        }
    }

    private static IdleStrategy performanceIdleStrategy() {
        return Boolean.getBoolean("spine.market.busySpin") ? new BusySpinIdleStrategy()
            : new BackoffIdleStrategy(100, 100, 1_000, 10_000);
    }

    public void connect() {
        connect(IPC);
    }

    public void connect(final String egressChannel) {
        client = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(aeronDirectory)
            .ingressChannel(IPC)
            .egressChannel(egressChannel)
            .messageTimeoutNs(TIMEOUT_NS)
            .errorHandler(errors::add)
            .egressListener((sessionId, timestamp, buffer, offset, length, header) -> {
                if (discardNextReply) {
                    discardNextReply = false;
                    return;
                }
                final byte[] copy = new byte[length];
                buffer.getBytes(offset, copy);
                replies.add(copy);
            }));
        assertHealthy();
    }

    public byte[] exchange(final DirectBuffer command, final int length) {
        client.pollEgress();
        final int before = replies.size();
        sendOnly(command, length);
        final long[] nextRetry = {System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250)};
        await(() -> replies.size() > before, () -> {
            // Client egress is best effort, including ADMIN_ACTION at a response term rollover.
            // Retain this exact command and keep one outstanding proposal until a receipt arrives.
            client.pollEgress();
            if (replies.size() == before && System.nanoTime() >= nextRetry[0]
                && client.offer(command, 0, length) > 0) {
                nextRetry[0] = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(250);
            }
        }, "committed proposal response");
        return replies.get(before);
    }

    public void sendOnly(final DirectBuffer command, final int length) {
        await(() -> client.offer(command, 0, length) > 0, "proposal publication");
    }

    public void discardNextReply() { discardNextReply = true; }
    public boolean replyWasDiscarded() { return !discardNextReply; }
    public Path clusterDirectory() { return clusterDirectory; }

    public String aeronDirectory() { return aeronDirectory; }
    public int archiveControlStreamId() { return archiveControlStreamId; }

    /** A separate producer owns this fresh context and its Cluster/Aeron transport lifecycle. */
    public AeronCluster.Context producerContext() {
        return new AeronCluster.Context().aeronDirectoryName(aeronDirectory)
            .ingressChannel(IPC).egressChannel(IPC).messageTimeoutNs(TIMEOUT_NS).errorHandler(errors::add);
    }

    public AeronArchive connectArchive(final int responseStreamId) {
        return AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(aeronDirectory)
            .controlRequestChannel(IPC).controlRequestStreamId(archiveControlStreamId)
            .controlResponseChannel(IPC).controlResponseStreamId(responseStreamId)
            .messageTimeoutNs(TIMEOUT_NS).errorHandler(errors::add));
    }

    public long inputRecordingId() {
        try (RecordingLog log = new RecordingLog(clusterDirectory.toFile(), false)) {
            return log.findLastTermRecordingId();
        }
    }

    public boolean snapshotInProgress() {
        return driver.consensusModule().context().moduleStateCounter().get() == ConsensusModule.State.SNAPSHOT.code();
    }

    public void takeSnapshot() {
        final long before = driver.consensusModule().context().snapshotCounter().get();
        final AtomicCounter toggle = controlToggle();
        await(() -> ClusterControl.ToggleState.SNAPSHOT.toggle(toggle), "snapshot request");
        await(() -> driver.consensusModule().context().snapshotCounter().get() > before
            && ClusterControl.ToggleState.get(toggle) == ClusterControl.ToggleState.NEUTRAL,
            "completed cluster snapshot");
    }

    /** ABORT preserves the previous snapshot so this restart must replay the committed suffix. */
    public void abort() {
        orderlyTermination = true;
        final AtomicCounter toggle = controlToggle();
        await(() -> ClusterControl.ToggleState.ABORT.toggle(toggle), "abort request");
        await(() -> consensusTerminated.get() && serviceTerminated.get(), "cluster and service termination");
    }

    public void shutdown() {
        orderlyTermination = true;
        final AtomicCounter toggle = controlToggle();
        await(() -> ClusterControl.ToggleState.SHUTDOWN.toggle(toggle), "shutdown request");
        await(() -> consensusTerminated.get() && serviceTerminated.get(), "snapshot shutdown termination");
    }

    /** A stop-replay acknowledgement can arrive after PersistentSubscription closes its private response session. */
    public void expectClosedConsumerControlResponse(final AeronArchive owner) {
        final String ownerSession = ChannelUri.parse(owner.context().controlResponseChannel()).get("session-id");
        final long ownerControlSession = owner.controlSessionId();
        final int stream = owner.context().controlResponseStreamId();
        expectClosedConsumerControlResponse(stream, ownerControlSession, ownerSession);
    }

    /** The same narrow acknowledgement warning, for an Archive client owned by a child JVM. */
    public void expectClosedConsumerControlResponse(final int stream, final long ownerControlSession,
                                            final String ownerSession) {
        expectedClosedConsumerWarning = expectedClosedConsumerWarning.or(error -> error instanceof ArchiveEvent event
            && event.category() == AeronException.Category.WARN
            && event.getMessage().startsWith("WARN - control response publication is not connected: ControlSession{controlSessionId=")
            && event.getMessage().contains("state=DONE,")
            && event.getMessage().contains("streamId=" + stream + ",")
            && !event.getMessage().contains("controlSessionId=" + ownerControlSession + ",")
            && (ownerSession == null || !event.getMessage().contains("sessionId=" + ownerSession + ",")));
    }

    public void assertHealthy() {
        for (Throwable failure; (failure = errors.peek()) != null; ) {
            if (!closedStartupKeepAlive && !driver.archive().context().aeron().isClosed()
                && isClosedNativeKeepAlive(failure, driver.archive().context().aeron().countersReader(),
                    driver.archive().context().archiveId(), driver.consensusModule().context().agentRoleName())) {
                closedStartupKeepAlive = true;
                errors.remove();
                continue;
            }
            if (expectedClosedConsumerWarning.test(failure)) {
                errors.remove();
                continue;
            }
            final IllegalStateException problem = new IllegalStateException("embedded Cluster infrastructure failed", failure);
            for (Throwable other : errors) { if (other != failure) problem.addSuppressed(other); }
            throw problem;
        }
    }

    /** Aeron 1.53.3's empty standby scan can close before its initial keepalive reaches the Archive. */
    static boolean isClosedNativeKeepAlive(final Throwable failure, final CountersReader counters,
                                          final long archiveId, final String consensusName) {
        final String prefix = "WARN - control request for unknown session: controlSessionId=";
        final String suffix = " templateId=61";
        if (!(failure instanceof ArchiveEvent event) || event.category() != AeronException.Category.WARN
            || !event.getMessage().startsWith(prefix) || !event.getMessage().endsWith(suffix)) { return false; }
        final long session;
        try { session = Long.parseLong(event.getMessage().substring(prefix.length(), event.getMessage().length() - suffix.length())); }
        catch (final NumberFormatException invalid) { return false; }
        for (int id = 0; id <= counters.maxCounterId(); id++) {
            if (counters.getCounterState(id) != CountersReader.RECORD_RECLAIMED
                || counters.getCounterTypeId(id) != ControlSessionCounter.TYPE_ID
                || counters.getCounterValue(id) != session) { continue; }
            // Agrona clears reclaimed keys; the value, type and label remain until this slot is reused.
            final String label = counters.getCounterLabel(id);
            if (label.startsWith("control-session: name=" + consensusName + " ")
                && label.endsWith(" - archiveId=" + archiveId)
                && counters.getCounterState(id) == CountersReader.RECORD_RECLAIMED
                && counters.getCounterValue(id) == session) { return true; }
        }
        return false;
    }

    public void pollAdmin() {
        if (client != null && !client.isClosed()) {
            client.pollEgress();
            final long now = System.nanoTime();
            if (now - lastKeepAliveNanos >= TimeUnit.MILLISECONDS.toNanos(500)) {
                client.sendKeepAlive();
                lastKeepAliveNanos = now;
            }
        }
    }

    private AtomicCounter controlToggle() {
        final AtomicCounter counter = ClusterControl.findControlToggle(driver.mediaDriver().context().countersManager(),
            driver.consensusModule().context().clusterId());
        if (counter == null) {
            throw new IllegalStateException("cluster control counter missing");
        }
        return counter;
    }

    private void await(final BooleanSupplier condition, final String description) {
        await(condition, () -> { }, description);
    }

    public void await(final BooleanSupplier condition, final Runnable pump, final String description) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (!condition.getAsBoolean()) {
            assertHealthy();
            pump.run();
            pollAdmin();
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("timed out awaiting " + description);
            }
            LockSupport.parkNanos(100_000);
        }
        assertHealthy();
    }

    /** Probe all sample-config UDP ports, then release immediately before launching the driver. */
    private static int availablePortBase() {
        for (int attempt = 0; attempt < 100; attempt++) {
            final int base = ThreadLocalRandom.current().nextInt(20_000, 60_000);
            final List<DatagramSocket> sockets = new ArrayList<>();
            try {
                for (int offset = 1; offset <= 5; offset++) {
                    final DatagramSocket socket = new DatagramSocket(null);
                    sockets.add(socket);
                    socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), base + offset));
                }
                return base;
            } catch (final java.io.IOException unavailable) {
                // Retry the entire range; a different test process may already own any one port.
            } finally {
                sockets.forEach(DatagramSocket::close);
            }
        }
        throw new IllegalStateException("could not reserve an isolated cluster UDP range");
    }

    @Override
    public void close() {
        try {
            CloseHelper.closeAll(client, driver.consensusModule(), container, driver.archive(), driver.mediaDriver());
        } finally {
            assertHealthy();
        }
    }
}
