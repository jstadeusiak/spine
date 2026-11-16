package io.github.jstadeusiak.spine.feeds;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.cluster.ClusterConfig;
import io.github.jstadeusiak.spine.client.CatalogSubscription;
import io.github.jstadeusiak.spine.client.EventHandler;
import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import io.github.jstadeusiak.spine.sequencer.SequencerService;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

/** Local persistent capture harness; one node segment plus public L2, or explicit public-only mode. */
public final class FeedMain {
    private static final String IPC = "aeron:ipc?term-length=1m";
    private static final long LOG = 42, TIMEOUT = 30_000_000_000L;

    public static void main(final String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            throw new IllegalArgumentException("DATA_DIR NODE_JSONL_OR_- [SECONDS=30] [COIN=BTC]; run the bundle jar");
        }
        final Path data = Path.of(args[0]).toAbsolutePath();
        final long seconds = args.length > 2 ? Long.parseLong(args[2]) : 30;
        if (seconds < 1 || seconds > 86_400) { throw new IllegalArgumentException("seconds must be 1..86400"); }
        final String coin = args.length > 3 ? args[3] : "BTC";
        final Path bundle = Path.of(FeedMain.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isRegularFile(bundle)) { throw new IllegalStateException("launch retained feeds bundle jar"); }
        final String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(bundle)));
        final Path artifact = data.resolve("artifacts").resolve(sha + ".jar");
        Files.createDirectories(artifact.getParent());
        if (!Files.exists(artifact)) { Files.copy(bundle, artifact); }
        if (!sha.equals(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(artifact))))) {
            throw new IllegalStateException("retained executable digest differs");
        }
        final SequencerService sequencer = new SequencerService();
        final ClusterConfig config = ClusterConfig.create(0, List.of("127.0.0.1"), List.of("127.0.0.1"),
            19_000, sequencer);
        sequencer.configure(config.consensusModuleContext(), config.clusteredServiceContext());
        final String directory = data.resolve("driver-" + UUID.randomUUID()).toString();
        config.baseDir(data.resolve("state").toFile());
        config.aeronDirectoryName(directory);
        config.mediaDriverContext().threadingMode(ThreadingMode.SHARED).ipcTermBufferLength(1 << 20)
            .ipcMtuLength(1024).publicationTermBufferLength(1 << 20).dirDeleteOnStart(true).dirDeleteOnShutdown(true);
        config.archiveContext().deleteArchiveOnStart(false).fileSyncLevel(0).localControlChannel(IPC).segmentFileLength(1 << 20);
        config.aeronArchiveContext().controlRequestChannel(IPC).controlResponseChannel(IPC);
        config.consensusModuleContext().archiveContext(config.aeronArchiveContext().clone()).deleteDirOnStart(false)
            .clusterClock(new NanosecondClusterClock()).ingressChannel(IPC).logChannel("aeron:udp?term-length=1m")
            .replayChannel(IPC).controlChannel(IPC).snapshotChannel("aeron:ipc?term-length=32m");
        config.clusteredServiceContext().archiveContext(config.aeronArchiveContext().clone())
            .replayChannel(IPC).controlChannel(IPC).snapshotChannel("aeron:ipc?term-length=32m");
        final long origin = System.nanoTime();
        final NodeFileCollector node = args[1].equals("-") ? null : new NodeFileCollector(Path.of(args[1]), origin);
        try (AutoCloseable nodeOwner = node == null ? null : node::onClose;
             ClusteredMediaDriver driver = ClusteredMediaDriver.launch(config.mediaDriverContext(), config.archiveContext(),
                 config.consensusModuleContext());
             ClusteredServiceContainer service = ClusteredServiceContainer.launch(config.clusteredServiceContext());
             AeronArchive archive = AeronArchive.connect(config.aeronArchiveContext().clone().controlResponseStreamId(8100))) {
            initialize(directory);
            try (CapturedIngress publicInput = ingress(archive, directory, 11, 8, null);
                 CapturedIngress nodeInput = node == null ? null : ingress(archive, directory, 10, 7, node)) {
                drain(publicInput);
                if (nodeInput != null) { drain(nodeInput); }
                final int clock = next(Math.max(publicInput.lastClockId(), nodeInput == null ? 0 : nodeInput.lastClockId()));
                final int publicConfig = next(publicInput.lastConfiguration());
                define(publicInput, publicConfig, clock, origin, artifact, sha, "public-api", coin,
                    "ws-session-" + Integer.toUnsignedString(publicConfig), false);
                if (nodeInput != null) {
                    final int nodeConfig = next(nodeInput.lastConfiguration());
                    define(nodeInput, nodeConfig, clock, origin, artifact, sha, "node-raw-book-diffs", "all-products",
                        node.session(), true);
                    node.attach(nodeInput, nodeConfig, clock);
                }
                final WebSocketCollector websocket = new WebSocketCollector(publicInput, publicConfig, clock, origin,
                    URI.create("wss://api.hyperliquid.xyz/ws"),
                    "{\"method\":\"subscribe\",\"subscription\":{\"type\":\"l2Book\",\"coin\":" + quote(coin) + ",\"fast\":true}}");
                try {
                    final BackoffIdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 100_000);
                    final long end = System.nanoTime() + seconds * 1_000_000_000L;
                    while (System.nanoTime() - end < 0) {
                        idle.idle(websocket.doWork() + (node == null ? 0 : node.doWork()));
                    }
                } finally { websocket.onClose(); }
                drain(publicInput);
                if (nodeInput != null) { drain(nodeInput); }
                final long head = Math.max(publicInput.acknowledgedSequence(), nodeInput == null ? 0 : nodeInput.acknowledgedSequence());
                final long[] frames = new long[2], bytes = new long[2];
                final CaptureCodec codec = new CaptureCodec();
                final EventHandler audit = (buffer, offset, length, event) -> {
                    final int body = offset + Wire.EVENT_OVERHEAD, size = length - Wire.EVENT_OVERHEAD;
                    if (codec.validObservation(buffer, body, size)) {
                        final int source = (int)event.sourceId() - 7;
                        if (source < 0 || source > 1) { throw new IllegalStateException("unexpected source"); }
                        frames[source]++; bytes[source] += codec.rawLength;
                    } else if (!codec.validDefinition(buffer, body, size)) { throw new IllegalStateException("unknown capture schema"); }
                };
                try (CatalogSubscription replay = new CatalogSubscription(archive, LOG, SequencerService.OUTPUT_STREAM,
                    0, IPC, 8300)) {
                    final long deadline = System.nanoTime() + TIMEOUT;
                    while (replay.sequence() < head) {
                        if (replay.requiresRefresh()) { replay.refresh(); }
                        replay.poll(audit, 256);
                        if (System.nanoTime() - deadline >= 0) { throw new IllegalStateException("recorded output replay incomplete"); }
                        LockSupport.parkNanos(100_000);
                    }
                    final String report = "{\"mode\":" + quote(node == null ? "public-only" : "node-and-public")
                        + ",\"sequence\":" + replay.sequence() + ",\"cumulativeNodeFrames\":" + frames[0]
                        + ",\"cumulativePublicFrames\":" + frames[1] + ",\"cumulativeNodeBytes\":" + bytes[0]
                        + ",\"cumulativePublicBytes\":" + bytes[1] + ",\"bundleSha256\":" + quote(sha) + "}";
                    Files.writeString(data.resolve("run-" + Integer.toUnsignedString(publicConfig) + ".json"), report + "\n");
                    System.out.println(report);
                }
            }
        }
    }

    private static CapturedIngress ingress(final AeronArchive archive, final String directory, final int producer,
        final int source, final NodeFileCollector node) {
        return new CapturedIngress(archive, context(directory), LOG, producer, source, 8200 + producer, 8400 + producer,
            node == null ? null : node::recovered);
    }
    private static AeronCluster.Context context(final String directory) {
        return new AeronCluster.Context().aeronDirectoryName(directory).ingressChannel(IPC).egressChannel(IPC).messageTimeoutNs(TIMEOUT);
    }
    private static void initialize(final String directory) {
        final Wire wire = new Wire();
        final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        final boolean[] accepted = {false};
        try (AeronCluster admin = AeronCluster.connect(context(directory).egressListener((session, time, buffer, offset, length, header) -> {
            if (!wire.validResult(buffer, offset, length) || wire.result.status() != AdmissionStatus.INITIALIZED) {
                throw new IllegalStateException("log initialization rejected");
            }
            accepted[0] = true;
        }))) {
            final int length = wire.encodeInitialize(command, 0, LOG);
            final long deadline = System.nanoTime() + TIMEOUT;
            long nextOffer = 0;
            while (!accepted[0]) {
                final long now = System.nanoTime();
                if ((nextOffer == 0 || now - nextOffer >= 0) && admin.offer(command, 0, length) > 0) { nextOffer = now + 250_000_000L; }
                admin.pollEgress();
                if (now - deadline >= 0) { throw new IllegalStateException("log initialization timeout"); }
                LockSupport.parkNanos(100_000);
            }
        }
    }
    private static void drain(final CapturedIngress ingress) {
        final long deadline = System.nanoTime() + TIMEOUT;
        while (!ingress.idle()) {
            ingress.poll();
            if (System.nanoTime() - deadline >= 0) { throw new IllegalStateException("capture/admission timeout"); }
            LockSupport.parkNanos(100_000);
        }
    }
    private static int next(final long value) {
        if (value >= 0xFFFF_FFFEL) { throw new IllegalStateException("source binding exhausted"); }
        return (int)(value + 1);
    }
    private static void define(final CapturedIngress ingress, final int configuration, final int clock, final long origin,
        final Path artifact, final String sha, final String provider, final String product, final String session, final boolean node) {
        final String entrypoint = node ? NodeFileCollector.class.getName() : WebSocketCollector.class.getName();
        final String profile = "{\"profile\":\"spine.capture.v1\",\"venue\":\"hyperliquid\",\"channel\":"
            + quote(node ? "raw_book_diffs" : "l2Book") + ",\"parser\":{\"artifact\":" + quote(artifact.toUri().toString())
            + ",\"sha256\":" + quote(sha) + ",\"entrypoint\":" + quote(entrypoint) + ",\"parameters\":{\"java\":"
            + quote(System.getProperty("java.vendor") + " " + System.getProperty("java.version"))
            + (node ? "" : ",\"fast\":true,\"nSigFigs\":null,\"mantissa\":null") + "}},\"raw\":{\"boundary\":"
            + quote(node ? "one LF-terminated segment line" : "one complete WebSocket application text message")
            + ",\"representation\":\"UTF-8\",\"transformation\":"
            + quote(node ? "LF removed; CR and all other bytes retained" : "JDK WebSocket UTF-16 callbacks reassembled and strictly reencoded UTF-8; RFC controls excluded")
            + "},\"native\":{\"namespace\":\"hyperliquid channel and source session\",\"representation\":\"absent; raw frame can contain several events\"},"
            + "\"clock\":{\"domain\":\"shared process monotonic elapsed nanoseconds\",\"origin\":" + quote(Long.toString(origin))
            + ",\"resolutionNanos\":1,\"receive\":" + quote(node ? "read containing first line byte" : "first text callback")
            + ",\"complete\":" + quote(node ? "LF observed by collector" : "last text callback assembled")
            + ",\"ready\":\"immediately before capture submission\"}}";
        final String[] labels = {provider, product, session, entrypoint, profile, "elapsed nanoseconds from retained shared origin; synchronization uncertainty unknown"};
        final byte[][] encoded = new byte[6][];
        for (int i = 0; i < labels.length; i++) { encoded[i] = labels[i].getBytes(StandardCharsets.UTF_8); }
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final int length = new CaptureCodec().encodeDefinition(payload, 0, -1, encoded);
        if (!ingress.submit(configuration, clock, -1, -1, System.nanoTime() - origin, payload, 0, length)) {
            throw new IllegalStateException("definition submission refused");
        }
        drain(ingress);
    }
    private static String quote(final String value) {
        final StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if (c == '"' || c == '\\') { result.append('\\').append(c); }
            else if (c < 32) { result.append(String.format("\\u%04x", (int)c)); }
            else { result.append(c); }
        }
        return result.append('"').toString();
    }
}
