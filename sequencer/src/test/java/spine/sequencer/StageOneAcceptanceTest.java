package spine.sequencer;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredService;
import spine.client.CatalogSubscription;
import spine.client.RecordingCatalog;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static spine.sequencer.StageOneWorker.*;
import static org.assertj.core.api.Assertions.assertThat;

/** Unchanged local fixed workload/budgets. These fixture observations do not establish HFT or host-loss readiness. */
class StageOneAcceptanceTest {
    private static final int INITIAL = 22, IDLE = 280, FINAL = 1_280;
    private static final int SUSTAINED_FIRST = 279, SUSTAINED_LAST = 1_278;
    private static final long STARTUP_NS = TimeUnit.SECONDS.toNanos(30), RECOVERY_NS = TimeUnit.SECONDS.toNanos(20);
    private static final String PROFILE = """
        {
          "name":"stage-one-local-v1","manifestVersion":1,"loadProfile":"stage-one-local-v1",
          "transportConfiguration":%s,
          "scope":"single-host single-node deterministic core; direct recorded output; external recorded capture; page-cache",
          "sustained":{"observations":1000,"scheduledRatePerSecond":100,"seconds":10,"maximumScheduleLatenessMs":100},
          "burst":{"observations":256,"completeCapturePayloadBytes":65536,"paced":false},
          "admission":{"p99Ms":500,"p999Ms":1000},"recorded":{"p99Ms":1000,"p999Ms":2000},
          "fastApplied":{"p99Ms":2000,"p999Ms":3000},
          "recoveryDeadlineSeconds":20,"minimumLiveCatchupPerSecond":200,"transportStartupSeconds":30,
          "parentObservedHeapMiB":768,"childObservedHeapMiB":192,"childMaximumHeapMiB":256,"maximumConcurrentChildren":3,
          "cpu":"diagnostic process CPU samples; no CPU acceptance gate",
          "latencyMethod":"BEGIN/GO before submit; parent-observed Archive follow/application stdout signals include control IO; admission uses producer-local monotonic elapsed time",
          "heapMethod":"cached parent MemoryMXBean; parent and child heap samples every 100ms; not allocation maxima",
          "lagMethod":"parent-sampled admitted cursor minus Archive-delivery cursor; includes replay scheduling",
          "idleHeadAuthority":"test parent Archive-follow cursor delivered over test control stdin while child data poll is paused; no production head beacon",
          "consumerTransport":"Archive REPLAY_ALL_AND_FOLLOW; no replay/live merger",
          "producerRetryIntervalNanos":10000000,
          "producerLossBoundary":"exact proposal completely recorded before Cluster offer; actual receipt completely recorded before next observation; abrupt JVM loss excludes host/power loss",
          "sourceDefinitions":[{"sourceId":1,"configurationVersion":1,"clockId":1,"provider":"fixture-provider-1"},
            {"sourceId":2,"configurationVersion":1,"clockId":1,"provider":"fixture-provider-2"}],
          "nativeIdentity":"paired numeric venue event IDs; numeric raw frame sequence and borrowed raw bytes",
          "clockMeaning":"ready UTC nanoseconds; receive/complete unavailable; uncertainty -1; no external clock sync claim"
        }
        """;
    private final String profile = profile();
    private final boolean performance = profile.equals("dedicated");
    private final String transport = """
        {"profile":"%s","mediaDriverThreadingMode":"%s","archiveThreadingMode":"DEDICATED",
        "clusterConsensusAndService":"separate agent threads","termBytes":1048576,"archiveSegmentBytes":1048576,
        "ipcMtuBytes":1024,"driverTimerIntervalNanos":5000000,"archiveFileSyncLevel":0,"corePinning":false,
        "idleStrategy":{"type":"BackoffIdleStrategy","maxSpins":%d,"maxYields":%d,"minParkNanos":1000,"maxParkNanos":%d}}
        """.formatted(profile, performance ? "DEDICATED" : "SHARED", performance ? 100 : 10,
            performance ? 100 : 20, performance ? 10_000 : 1_000_000);
    private final List<Child> children = new ArrayList<>();
    private final Map<Integer, Long> starts = new HashMap<>(), admissions = new HashMap<>(),
        recordings = new HashMap<>(), applications = new HashMap<>();
    private final ConcurrentLinkedQueue<Receipt> receipts = new ConcurrentLinkedQueue<>();
    private final List<String> stages = new ArrayList<>();
    private final Wire wire = new Wire();
    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    private ClusterFixture fixture;
    private AeronArchive archive;
    private CatalogSubscription recorded;
    private Child fast;
    private Path directory;
    private long seenRecorded, sentHead, peakParentHeap;
    private long nextHeapSample;
    private volatile long admittedCursor;
    private long observedArchiveLag;
    private long sustainedStart, sustainedLastBegin, sustainedLastAdmission, sustainedLastRecorded;
    private long idleRecovery, liveRecovery, liveRecoveryObservedAt, scheduleLateness, sustainedElapsed;
    private double catchupRate;
    private String finalDigest = "", continuedDigest = "", pendingDigest = "";
    private long firstGeneration = -1, secondGeneration = -1;
    private final long started = System.nanoTime();
    private final long cpuStarted = ProcessHandle.current().info().totalCpuDuration().orElseThrow().toNanos();

    @Test
    @Timeout(240)
    void separateProcessesCaptureCrashRestartAndReconstructWithinTheDeclaredEnvelope() throws Exception {
        final Path output = Path.of(System.getProperty("spine.acceptance.output", "build/reports/stage-one"));
        Files.createDirectories(output);
        final Path run = Files.createDirectories(output.resolve("run-" + UUID.randomUUID()));
        directory = Files.createDirectories(run.resolve("data"));
        Files.writeString(run.resolve("profile.json"), PROFILE.formatted(transport), StandardOpenOption.CREATE_NEW);
        try {
            run(run);
            report(output, run, "PASSED", "");
        } catch (final Throwable failure) {
            report(output, run, "FAILED", failure.toString());
            throw failure;
        } finally {
            for (final Child child : children) { child.forceClose(); }
            CloseHelper.quietCloseAll(recorded, archive, fixture);
        }
    }

    private void run(final Path run) throws Exception {
        fixture = new ClusterFixture(directory, observedService(), true, performance);
        initialize();
        archive = fixture.connectArchive(8101);
        recorded = new CatalogSubscription(archive, LOG, DATA_STREAM, 0, IPC, 9201);
        firstGeneration = catalog(9401).spans().getFirst().recordingId();
        final Child initialProducer = producer(run, "producer-first", 8110);
        fast = consumer(run, "fast", 8102, 9301, directory.resolve("fast.checkpoint"), false);
        final Child slow = consumer(run, "slow", 8103, 9302, directory.resolve("slow.checkpoint"), false);
        ready(initialProducer); ready(fast); ready(slow);
        initialProducer.send("DEFINE");
        waitFor(() -> initialProducer.has("DONE DEFINE"), STARTUP_NS, "both source definitions admitted");
        fence(fast, INITIAL); fence(slow, INITIAL);
        initialProducer.send("CAPTURE 1 20 4096 0");
        waitFor(() -> fast.report(INITIAL) != null && slow.report(INITIAL) != null, STARTUP_NS, "initial live fence");
        compare(fast.report(INITIAL), slow.report(INITIAL), INITIAL);
        slow.send("CHECKPOINT");
        waitFor(() -> slow.has("CHECKPOINT " + INITIAL), STARTUP_NS, "application checkpoint prefix");
        slow.send("PAUSE");
        waitFor(() -> slow.has("PAUSED " + INITIAL), STARTUP_NS, "independent slow data pause");
        initialProducer.send("LOSS 21");
        waitFor(() -> initialProducer.has("LOST 23") && seenRecorded == 23, STARTUP_NS, "actual accepted receipt discarded after output recording");
        final String[] pending = initialProducer.find("PERSISTED ").split(" ");
        pendingDigest = pending[1];
        final long lostSequence = Long.parseLong(pending[3]);
        final Receipt accepted = receipt(10, lostSequence, AdmissionStatus.ACCEPTED);
        initialProducer.kill();
        final Child producer = producer(run, "producer-reopened", 8111);
        ready(producer);
        assertThat(producer.find("IDENTITIES ")).isEqualTo(initialProducer.find("IDENTITIES "));
        assertThat(producer.find("PENDING ")).isEqualTo("PENDING " + pendingDigest);
        assertThat(producer.has("RECOVERED 23")).isTrue();
        assertThat(receipt(10, lostSequence, AdmissionStatus.DUPLICATE).canonical).containsExactly(accepted.canonical);
        assertThat(seenRecorded).isEqualTo(23);
        stages.add("collector killed after lost ACCEPTED; external capture restores exact pending bytes/identity and original DUPLICATE event");
        producer.send("CAPTURE 22 22 65536 0");
        waitFor(() -> producer.has("DONE CAPTURE 22 ") && seenRecorded == 24, STARTUP_NS, "complete native pair before burst");
        fence(fast, IDLE);
        producer.send("CAPTURE 23 278 65536 0");
        waitFor(() -> producer.has("DONE CAPTURE 278 ") && fast.report(IDLE) != null, STARTUP_NS, "256 maximum-payload burst");
        waitFor(() -> slow.headAtLeast(IDLE), RECOVERY_NS, "independent recorded cursor reveals missing idle final data");
        fence(slow, IDLE); slow.send("RESUME");
        waitFor(() -> slow.report(IDLE) != null && slow.recovery(INITIAL) != null, RECOVERY_NS, "Archive follow reaches idle final fence without new input");
        idleRecovery = Long.parseLong(slow.recovery(INITIAL).split(" ")[3]);
        compare(fast.report(IDLE), slow.report(IDLE), IDLE);
        stages.add("paused data reaches the recorded idle final fence through independent Archive delivery without another observation");
        // Keep this earlier baseline: restart must regenerate the full committed suffix and verify output overlap.
        fixture.takeSnapshot();
        slow.send("PAUSE");
        waitFor(() -> slow.has("PAUSED " + IDLE), STARTUP_NS, "pause before sustained scheduled load");
        fence(fast, FINAL); fence(slow, FINAL);
        producer.send("CAPTURE 279 1278 4096 10000000");
        waitFor(() -> slow.headAtLeast(IDLE + 400), RECOVERY_NS, "400-event recorded live backlog");
        slow.send("RESUME");
        waitFor(() -> slow.recovery(IDLE) != null, RECOVERY_NS, "catch up while scheduled input continues");
        assertThat(producer.has("DONE CAPTURE 1278 ")).as("producer still active when catch-up finishes").isFalse();
        final String[] recovery = slow.recovery(IDLE).split(" ");
        liveRecovery = Long.parseLong(recovery[3]);
        final long caught = Long.parseLong(recovery[2]) - IDLE;
        assertThat(caught).isGreaterThanOrEqualTo(400);
        catchupRate = caught * 1_000_000_000.0 / liveRecovery;
        stages.add("400-event Archive backlog drained while the actual fixed-rate producer remains active");
        waitFor(() -> producer.has("DONE CAPTURE 1278 ") && fast.report(FINAL) != null && slow.report(FINAL) != null,
            STARTUP_NS, "1000 sustained observations completely recorded and applied");
        final String[] sustained = producer.find("DONE CAPTURE 1278 ").split(" ");
        assertThat(liveRecoveryObservedAt).isPositive();
        assertThat(sustainedLastBegin).as("an actual later BEGIN/GO proves input continued after observed recovery")
            .isGreaterThan(liveRecoveryObservedAt);
        sustainedElapsed = Long.parseLong(sustained[3]);
        scheduleLateness = Long.parseLong(sustained[4]);
        for (final Map<Integer, Long> values : List.of(starts, admissions, recordings, applications)) {
            assertThat(values.keySet().stream().filter(StageOneAcceptanceTest::isSustained).count()).isEqualTo(1000);
        }
        compare(fast.report(FINAL), slow.report(FINAL), FINAL);
        finalDigest = fast.report(FINAL).split(" ")[5];
        stop(producer); stop(fast); stop(slow);
        recorded.close(); recorded = null;
        fixture.abort();
        archive.close(); archive = null;
        fixture.close(); fixture = null;

        fixture = new ClusterFixture(directory, observedService(), false, performance);
        initialize();
        archive = fixture.connectArchive(8201);
        final RecordingCatalog restartedCatalog = catalog(9600);
        assertThat(restartedCatalog.headSequence()).isEqualTo(FINAL);
        assertThat(restartedCatalog.spans()).hasSize(2);
        final RecordingCatalog.Span replayed = restartedCatalog.spans().getLast();
        secondGeneration = replayed.recordingId();
        assertThat(secondGeneration).isNotEqualTo(firstGeneration);
        assertThat(replayed.firstSequence()).isEqualTo(IDLE + 1);
        assertThat(replayed.lastSequence()).isEqualTo(FINAL);
        stages.add("snapshot at 280 plus ABORT at 1280 replays one authoritative input suffix; 1000 repeated output frames are byte-verified");
        recorded = new CatalogSubscription(archive, LOG, DATA_STREAM, FINAL, IPC, 9202);
        sentHead = 0;
        final Child clean = consumer(run, "clean-replay", 8202, 9501, directory.resolve("unused.checkpoint"), false);
        final Child restored = consumer(run, "checkpoint-restored", 8203, 9502, directory.resolve("slow.checkpoint"), true);
        ready(clean); ready(restored);
        clean.send("HEAD " + FINAL); restored.send("HEAD " + FINAL);
        fence(clean, FINAL); fence(restored, FINAL);
        waitFor(() -> clean.report(FINAL) != null && restored.report(FINAL) != null, RECOVERY_NS, "clean and checkpoint reconstruction");
        compare(clean.report(FINAL), restored.report(FINAL), FINAL);
        assertThat(clean.report(FINAL).split(" ")[5]).isEqualTo(finalDigest);
        final Child continuation = producer(run, "producer-after-cluster-restart", 8210);
        ready(continuation);
        assertThat(continuation.find("IDENTITIES ")).isEqualTo("IDENTITIES 10 11");
        fence(clean, FINAL + 2); fence(restored, FINAL + 2);
        continuation.send("CAPTURE 1279 1280 4096 0");
        waitFor(() -> clean.report(FINAL + 2) != null && restored.report(FINAL + 2) != null,
            RECOVERY_NS, "same producer progress continues canonical history");
        compare(clean.report(FINAL + 2), restored.report(FINAL + 2), FINAL + 2);
        continuedDigest = clean.report(FINAL + 2).split(" ")[5];
        assertThat(catalog(9700).headSequence()).isEqualTo(FINAL + 2);
        stages.add("clean/checkpoint state and ordered full-frame SHA match; captured producer progress continues to 1282");
        stop(continuation); stop(clean); stop(restored);
        pump();
        // Preserve all recovery evidence before evaluating the unchanged fixed capacity/latency budgets.
        assertThat(scheduleLateness).as("maximum fixed-schedule lateness").isLessThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(100));
        assertThat(idleRecovery).isLessThanOrEqualTo(RECOVERY_NS);
        assertThat(liveRecovery).isLessThanOrEqualTo(RECOVERY_NS);
        assertThat(catchupRate).isGreaterThanOrEqualTo(200);
        latency("admission", admissions, 500, 1000);
        latency("recorded", recordings, 1000, 2000);
        latency("fast application", applications, 2000, 3000);
        assertThat(peakParentHeap).isLessThanOrEqualTo(768L * 1024 * 1024);
        for (final Child child : children) {
            assertThat(child.peakHeap).as(child.name + " observed samples").isPositive().isLessThanOrEqualTo(192L * 1024 * 1024);
        }
        fixture.assertHealthy();
    }

    private Receipt receipt(final long producer, final long sequence, final AdmissionStatus status) {
        return receipts.stream().filter(value -> value.producer == producer && value.sequence == sequence && value.status == status)
            .findFirst().orElseThrow();
    }

    private ClusteredService observedService() {
        final SequencerService delegate = new SequencerService();
        final Wire receiptWire = new Wire();
        return (ClusteredService)Proxy.newProxyInstance(ClusteredService.class.getClassLoader(), new Class<?>[]{ClusteredService.class},
            (proxy, method, arguments) -> {
                Object[] forwarded = arguments;
                if (method.getName().equals("onSessionMessage") && arguments[0] instanceof ClientSession session) {
                    forwarded = arguments.clone();
                    forwarded[0] = Proxy.newProxyInstance(ClientSession.class.getClassLoader(), new Class<?>[]{ClientSession.class},
                        (sessionProxy, sessionMethod, values) -> {
                            if (sessionMethod.getName().equals("offer") && values.length == 3 && values[0] instanceof DirectBuffer buffer) {
                                final int offset = (int)values[1], length = (int)values[2];
                                if (!receiptWire.validResult(buffer, offset, length)) { throw new AssertionError("invalid actual service receipt"); }
                                final byte[] canonical = new byte[length - Wire.RESULT_OVERHEAD];
                                buffer.getBytes(offset + Wire.RESULT_OVERHEAD, canonical);
                                if (canonical.length != 0) { admittedCursor = Math.max(admittedCursor, receiptWire.event.sequence()); }
                                receipts.add(new Receipt(receiptWire.result.producerId(), receiptWire.result.producerSequence(), receiptWire.result.status(), canonical));
                            }
                            try { return sessionMethod.invoke(session, values); }
                            catch (final InvocationTargetException error) { throw error.getCause(); }
                        });
                }
                try { return method.invoke(delegate, forwarded); }
                catch (final InvocationTargetException error) { throw error.getCause(); }
            });
    }

    private void initialize() {
        fixture.connect();
        final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        wire.encodeInitialize(command, 0, LOG);
        final byte[] result = fixture.exchange(command, command.capacity());
        assertThat(wire.validResult(new UnsafeBuffer(result), 0, result.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(AdmissionStatus.INITIALIZED);
    }

    private RecordingCatalog catalog(final int stream) { return RecordingCatalog.rebuild(archive, LOG, DATA_STREAM, IPC, stream); }
    private Child producer(final Path run, final String name, final int response) throws Exception {
        return start(run, name, "producer", fixture.aeronDirectory(), Integer.toString(fixture.archiveControlStreamId()), Integer.toString(response));
    }
    private Child consumer(final Path run, final String name, final int response, final int replay, final Path checkpoint, final boolean restore) throws Exception {
        return start(run, name, "consumer", fixture.aeronDirectory(), Integer.toString(fixture.archiveControlStreamId()),
            Integer.toString(response), Integer.toString(replay), checkpoint.toString(), Boolean.toString(restore));
    }
    private Child start(final Path run, final String name, final String... args) throws Exception {
        assertThat(children.stream().filter(child -> child.process.isAlive()).count()).isLessThan(3);
        final List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx256m", "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens", "java.base/java.util.zip=ALL-UNNAMED",
            "-cp", System.getProperty("spine.acceptance.classpath"), StageOneWorker.class.getName()));
        command.addAll(List.of(args));
        final Child child = new Child(name, new ProcessBuilder(command).redirectError(run.resolve(name + ".stderr.log").toFile()).start(), run);
        children.add(child); return child;
    }
    private void ready(final Child child) throws Exception { waitFor(() -> child.has("READY "), STARTUP_NS, child.name + " startup"); }
    private void fence(final Child child, final long fence) throws Exception { child.send("FENCE " + fence); }
    private void stop(final Child child) throws Exception {
        child.stopping = true; child.send("STOP");
        waitFor(() -> !child.process.isAlive(), STARTUP_NS, child.name + " orderly close");
        assertThat(child.process.exitValue()).as(child.name + " exit; " + child.errorFile).isZero();
    }
    private void waitFor(final BooleanSupplier done, final long timeout, final String description) throws Exception {
        final long deadline = System.nanoTime() + timeout;
        while (true) {
            pump();
            if (done.getAsBoolean()) { return; }
            if (System.nanoTime() >= deadline) { throw new AssertionError("stage deadline: " + description); }
            LockSupport.parkNanos(100_000);
        }
    }

    private void pump() throws Exception {
        for (final Child child : children) {
            for (Signal signal; (signal = child.signals.poll()) != null;) {
                child.messages.add(signal.line);
                final String[] values = signal.line.split(" ");
                switch (values[0]) {
                    case "BEGIN" -> {
                        final int observation = Integer.parseInt(values[1]);
                        if (observation > 0) {
                            starts.put(observation, System.nanoTime());
                            if (isSustained(observation)) {
                                if (sustainedStart == 0) { sustainedStart = starts.get(observation); }
                                sustainedLastBegin = starts.get(observation);
                            }
                        }
                        child.send("GO " + observation);
                    }
                    case "ACK" -> {
                        final int observation = Integer.parseInt(values[1]);
                        if (observation > 22 && observation <= SUSTAINED_LAST) { admissions.put(observation, Long.parseLong(values[3])); }
                        if (isSustained(observation)) { sustainedLastAdmission = signal.observedAt; }
                    }
                    case "APPLIED" -> {
                        final int observation = Integer.parseInt(values[1]) - 2;
                        if (child == fast && observation > 22 && observation <= SUSTAINED_LAST && starts.containsKey(observation)) {
                            applications.putIfAbsent(observation, signal.observedAt - starts.get(observation));
                        }
                    }
                    case "RECOVERY" -> { if (child.consumer && Long.parseLong(values[1]) == IDLE) { liveRecoveryObservedAt = signal.observedAt; } }
                    case "SAMPLE" -> { child.peakHeap = Math.max(child.peakHeap, Long.parseLong(values[1])); child.cpu = Long.parseLong(values[2]); }
                    case "CONTROL" -> fixture.expectClosedConsumerControlResponse(Integer.parseInt(values[1]), Long.parseLong(values[2]),
                        values[3].equals("none") ? null : values[3]);
                    default -> { }
                }
            }
            if (!child.stopping && !child.process.isAlive()) { throw new AssertionError(child.name + " exited " + child.process.exitValue() + ": " + Files.readString(child.errorFile)); }
        }
        final long heapSampleTime = System.nanoTime();
        if (nextHeapSample == 0 || heapSampleTime - nextHeapSample >= 0) {
            peakParentHeap = Math.max(peakParentHeap, memory.getHeapMemoryUsage().getUsed());
            nextHeapSample = heapSampleTime + 100_000_000;
        }
        if (fixture != null) { fixture.pollAdmin(); fixture.assertHealthy(); }
        observedArchiveLag = Math.max(observedArchiveLag, Math.max(0, admittedCursor - seenRecorded));
        if (recorded != null) {
            recorded.poll((buffer, offset, length, event) -> {
                final long now = System.nanoTime();
                final int observation = (int)event.sequence() - 2;
                seenRecorded = event.sequence();
                if (observation > 22 && observation <= SUSTAINED_LAST && starts.containsKey(observation)) {
                    recordings.putIfAbsent(observation, now - starts.get(observation));
                }
                if (isSustained(observation)) { sustainedLastRecorded = now; }
            }, 512);
        }
        if (seenRecorded != sentHead) {
            for (final Child child : children) {
                if (child.process.isAlive() && !child.stopping && child.consumer) { child.send("HEAD " + seenRecorded); }
            }
            sentHead = seenRecorded;
        }
    }

    private static void compare(final String first, final String second, final long fence) {
        final String[] a = first.split(" "), b = second.split(" ");
        assertThat(java.util.Arrays.copyOfRange(a, 1, 7)).containsExactly(java.util.Arrays.copyOfRange(b, 1, 7));
        assertThat(Long.parseLong(a[1])).isEqualTo(fence);
        assertThat(Long.parseLong(a[3])).isEqualTo(fence - 2);
        assertThat(Long.parseLong(a[4])).isEqualTo((fence - 2) / 2);
        final long pairs = (fence - 2) / 2;
        assertThat(Long.parseLong(a[2])).isEqualTo(pairs * (pairs + 1));
        assertThat(Long.parseLong(a[6])).isEqualTo(fence);
    }
    private static boolean isSustained(final int observation) { return observation >= SUSTAINED_FIRST && observation <= SUSTAINED_LAST; }
    private static long percentile(final Map<Integer, Long> values, final double percentile) {
        final long[] sorted = values.values().stream().mapToLong(Long::longValue).sorted().toArray();
        return sorted.length == 0 ? 0 : sorted[(int)Math.ceil(sorted.length * percentile) - 1];
    }
    private static void latency(final String label, final Map<Integer, Long> values, final long p99, final long p999) {
        assertThat(values.size()).as(label + " complete samples").isEqualTo(1256);
        assertThat(percentile(values, .99)).as(label + " p99").isLessThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(p99));
        assertThat(percentile(values, .999)).as(label + " p99.9").isLessThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(p999));
    }

    private void report(final Path output, final Path run, final String status, final String failure) throws Exception {
        final String resources = children.stream().map(child -> "{\"process\":" + json(child.name) + ",\"observedPeakHeapBytes\":"
            + child.peakHeap + ",\"sampledCpuNanos\":" + child.cpu + "}").collect(java.util.stream.Collectors.joining(",", "[", "]"));
        final String report = String.format(java.util.Locale.ROOT, """
            {"status":%s,"failure":%s,"profile":%s,"dataDirectory":%s,"transportConfiguration":%s,
            "runtime":{"java":%s,"os":%s,"arch":%s,"processors":%d},
            "versions":{"canonicalSchemaId":101,"captureSchemaId":201,"captureVersion":1,"interpreter":"stage-one-capture-projection-v1"},
            "scope":"synthetic single-host single-node page-cache; no power-loss, host-loss or HFT claim",
            "logId":42,"declaredFences":{"initial":22,"idle":280,"replay":1280,"continued":1282},
            "observedRecordedCursor":%d,"observedArchiveDeliveryLagEvents":%d,"orderedFullFrameSha256":%s,"continuedSha256":%s,"exactPendingSha256":%s,
            "recordingGenerations":[%d,%d],"completedStages":%s,"admission":%s,"recorded":%s,"fastApplied":%s,
            "sustained":{"observations":1000,"actualBeginRate":%f,"actualAdmissionRate":%f,"actualRecordedRate":%f,
            "workerElapsedNanos":%d,"maximumScheduleLatenessNanos":%d},
            "idleRecoveryNanos":%d,"liveRecoveryNanos":%d,"liveCatchupObservationsPerSecond":%f,
            "parentObservedPeakHeapBytes":%d,"parentSampledCpuNanos":%d,"parentElapsedNanos":%d,"cpuGate":false,"children":%s,
            "measurementMethod":"See immutable profile; no cross-JVM monotonic timestamp subtraction; actual Archive bytes establish recording observations."}
            """, json(status), json(failure), json(run.resolve("profile.json").toAbsolutePath().toString()),
            json(directory.toAbsolutePath().toString()), transport, json(System.getProperty("java.runtime.version")),
            json(System.getProperty("os.name")), json(System.getProperty("os.arch")), Runtime.getRuntime().availableProcessors(),
            seenRecorded, observedArchiveLag, json(finalDigest), json(continuedDigest), json(pendingDigest), firstGeneration, secondGeneration,
            stages.stream().map(StageOneAcceptanceTest::json).collect(java.util.stream.Collectors.joining(",", "[", "]")),
            latencyJson(admissions), latencyJson(recordings), latencyJson(applications), rate(sustainedLastBegin), rate(sustainedLastAdmission),
            rate(sustainedLastRecorded), sustainedElapsed, scheduleLateness, idleRecovery, liveRecovery, catchupRate, peakParentHeap,
            ProcessHandle.current().info().totalCpuDuration().orElseThrow().toNanos() - cpuStarted, System.nanoTime() - started, resources);
        Files.writeString(run.resolve("report.json"), report);
        Files.writeString(output.resolve("report.json"), report);
    }
    private double rate(final long last) { return last > sustainedStart ? 999 * 1_000_000_000.0 / (last - sustainedStart) : 0; }
    private static String latencyJson(final Map<Integer, Long> values) {
        return "{\"samples\":%d,\"p50Nanos\":%d,\"p99Nanos\":%d,\"p999Nanos\":%d}".formatted(values.size(),
            percentile(values, .5), percentile(values, .99), percentile(values, .999));
    }
    private static String profile() {
        final String value = System.getProperty("spine.acceptance.aeron.profile", "shared");
        if (!value.equals("shared") && !value.equals("dedicated")) { throw new IllegalArgumentException("shared or dedicated required"); }
        return value;
    }
    private static String json(final String value) {
        final StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            switch (c) {
                case '\\' -> result.append("\\\\"); case '"' -> result.append("\\\"");
                case '\n' -> result.append("\\n"); case '\r' -> result.append("\\r"); case '\t' -> result.append("\\t");
                default -> { if (c < 32) { result.append("\\u%04x".formatted((int)c)); } else { result.append(c); } }
            }
        }
        return result.append('"').toString();
    }
    private record Signal(String line, long observedAt) { }
    private record Receipt(long producer, long sequence, AdmissionStatus status, byte[] canonical) { }

    private static final class Child {
        final String name;
        final Process process;
        final BufferedWriter input;
        final ConcurrentLinkedQueue<Signal> signals = new ConcurrentLinkedQueue<>();
        final List<String> messages = new ArrayList<>();
        final Path errorFile;
        final boolean consumer;
        long peakHeap, cpu;
        boolean stopping;

        Child(final String name, final Process process, final Path run) throws Exception {
            this.name = name; this.process = process; consumer = !name.startsWith("producer");
            errorFile = run.resolve(name + ".stderr.log");
            input = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
            final Thread reader = new Thread(() -> {
                try (BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
                     BufferedWriter log = Files.newBufferedWriter(run.resolve(name + ".stdout.log"))) {
                    for (String line; (line = stdout.readLine()) != null;) {
                        final long time = System.nanoTime(); log.write(line); log.newLine(); log.flush(); signals.add(new Signal(line, time));
                    }
                } catch (final Exception failure) { signals.add(new Signal("READER_FAILED " + failure, System.nanoTime())); }
            }, "stage-one-" + name);
            reader.setDaemon(true); reader.start();
        }
        void send(final String value) throws Exception { input.write(value); input.newLine(); input.flush(); }
        boolean has(final String prefix) { return messages.stream().anyMatch(value -> value.startsWith(prefix)); }
        String find(final String prefix) { return messages.stream().filter(value -> value.startsWith(prefix)).findFirst().orElseThrow(); }
        String report(final long fence) { return messages.stream().filter(value -> value.startsWith("REPORT " + fence + " ")).findFirst().orElse(null); }
        String recovery(final long from) { return messages.stream().filter(value -> value.startsWith("RECOVERY " + from + " ")).findFirst().orElse(null); }
        boolean headAtLeast(final long head) { return messages.stream().filter(value -> value.startsWith("BEHIND "))
            .anyMatch(value -> Long.parseLong(value.split(" ")[2]) >= head); }
        void kill() throws Exception {
            stopping = true; process.destroyForcibly();
            if (!process.waitFor(30, TimeUnit.SECONDS)) { throw new AssertionError("collector kill timeout"); }
            assertThat(process.exitValue()).isNotZero();
        }
        void forceClose() {
            stopping = true;
            if (process.isAlive()) { process.destroyForcibly(); }
            try { process.waitFor(10, TimeUnit.SECONDS); input.close(); } catch (final Exception ignored) { }
        }
    }
}
