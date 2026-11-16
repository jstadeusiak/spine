package io.github.jstadeusiak.spine.examples.binance;

import io.github.jstadeusiak.spine.sequencer.SequencerService;
import io.github.jstadeusiak.spine.sequencer.ClusterFixture;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.client.CatalogSubscription;
import io.github.jstadeusiak.spine.client.EventHandler;
import io.github.jstadeusiak.spine.client.ReliableProducer;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.market.sbe.BookResultDecoder;
import io.github.jstadeusiak.spine.protocol.market.sbe.BookSourceEncoder;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import io.github.jstadeusiak.spine.protocol.sbe.ObservationProposalEncoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Real capture and output recording authorities, plus two different market snapshot/replay boundaries. */
class MarketBookIntegrationTest {
    private static final String IPC = "aeron:ipc?term-length=1m";
    private static final String CAPTURE_CHANNEL = IPC + "|alias=spine-book-capture";
    private static final int CAPTURE_STREAM = 7441;
    private static final long LOG = MarketCoreTest.LOG, UNIT = MarketCoreTest.UNIT;
    @TempDir Path directory;

    @Test @Timeout(240)
    void recordedBooksRecoverMidBootstrapAndPendingGapWithoutASecondCommitOrLostReceiptMutation() throws Exception {
        final MarketCoreTest.Inputs inputs = new MarketCoreTest.Inputs();
        final List<byte[]> expected = new ArrayList<>(), captured = new ArrayList<>();
        final List<CaptureRun> recordings = new ArrayList<>();
        final byte[] bootstrap = bootstrap();
        final int maximum = Wire.MAX_PAYLOAD_BYTES - 28;
        final ObservedService first = new ObservedService();
        try (ClusterFixture fixture = new ClusterFixture(directory, first, true)) {
            initialize(fixture);
            try (Run run = new Run(fixture, first, new long[] {0, 1, 1, 1}, 0, 0, expected, captured, recordings)) {
                for (int source = 1; source <= 3; source++) {
                    final UnsafeBuffer binding = inputs.bind(source, source, 1, 1, 1);
                    new BookSourceEncoder().wrap(binding, Wire.PROPOSAL_OVERHEAD + Wire.HEADER_LENGTH)
                        .maximumAgeNanos(60_000_000_000L); // Recovery tests permit slow snapshots; freshness has its own gate.
                    run.send(binding);
                }
                run.send(inputs.chunk(3, 1, 1, 1, bootstrap.length, 0, Arrays.copyOf(bootstrap, maximum)));
                assertBook(expected.getLast(), 0, BinanceBook.EMPTY, 0);
                fixture.takeSnapshot(); // Includes one retained bootstrap fragment, before any book installation.
            }
            fixture.abort();
            assertThat(first.selfOffers.get()).isZero();
        }

        final ObservedService second = new ObservedService();
        try (ClusterFixture fixture = new ClusterFixture(directory, second, false)) {
            initialize(fixture);
            assertThat(second.restored).isTrue();
            try (Run run = new Run(fixture, second, new long[] {0, 2, 2, 3}, expected.size(), 1,
                expected, captured, recordings)) {
                for (int offset = maximum; offset < bootstrap.length;) {
                    final int size = Math.min(maximum, bootstrap.length - offset);
                    run.send(inputs.chunk(3, 1, 1, 1, bootstrap.length, offset,
                        Arrays.copyOfRange(bootstrap, offset, offset + size)));
                    offset += size;
                    assertBook(expected.getLast(), offset == bootstrap.length ? 100 : 0,
                        offset == bootstrap.length ? BinanceBook.VALID : BinanceBook.EMPTY,
                        offset == bootstrap.length ? UNIT : 0);
                }
                run.send(inputs.observation(1, 1, 1, MarketCoreTest.utf8(diff(101, "2"))));
                run.send(inputs.observation(2, 1, 1, sbe(101, 2)));
                assertBook(expected.getLast(), 101, BinanceBook.VALID, 2 * UNIT);
                run.send(inputs.observation(1, 1, 1, MarketCoreTest.utf8(diff(103, "3"))));
                assertBook(expected.getLast(), 101, BinanceBook.GAP, 0);
                fixture.takeSnapshot(); // Pending native103 survives while frontier101 remains ineligible.

                final UnsafeBuffer repair = inputs.observation(2, 1, 1, sbe(102, 4));
                second.holdProducer = 12;
                second.holdSequence = repair.getLong(24, Wire.ORDER);
                second.hold = true;
                final long repairCursor = run.consumer.sequence() + 1;
                final ReliableProducer sender = run.stage(repair);
                fixture.await(() -> second.heldReceipt != null, () -> run.pump(sender, true), "held book receipt");
                run.follow(repairCursor, sender, false);
                assertThat(sender.pendingLength()).isEqualTo(repair.capacity());
                assertBook(expected.getLast(), 103, BinanceBook.VALID, 3 * UNIT);

                // A later producer changes state while the repair producer owns its exact captured retry.
                run.send(inputs.observation(1, 1, 1, MarketCoreTest.utf8(diff(104, "5"))));
                assertBook(expected.getLast(), 104, BinanceBook.VALID, 5 * UNIT);
                second.hold = false;
                assertThat(sender.retryPending()).isTrue();
                fixture.await(() -> sender.pendingLength() == 0, () -> run.pump(sender, true), "cached book retry receipt");
                assertThat(sender.lastStatus()).isEqualTo(AdmissionStatus.DUPLICATE);
                assertThat(second.retryReceipt).isNotNull();
                assertThat(canonical(second.retryReceipt)).isEqualTo(canonical(second.heldReceipt));
                assertBook(canonical(second.retryReceipt), 103, BinanceBook.VALID, 3 * UNIT);
                assertThat(run.consumer.sequence()).isEqualTo(expected.size());
            }
            fixture.abort(); // The pending-gap snapshot has a committed repair/retry/later-update suffix.
            assertThat(second.selfOffers.get()).isZero();
        }

        final ObservedService third = new ObservedService();
        final boolean scheduled = Boolean.getBoolean("spine.market.scheduled");
        final boolean profile = Boolean.getBoolean("spine.market.profile") || scheduled;
        try (ClusterFixture fixture = new ClusterFixture(directory, third, false, profile)) {
            initialize(fixture);
            assertThat(third.restored).isTrue();
            final List<byte[]> recovered = new ArrayList<>();
            try (Run run = new Run(fixture, third,
                new long[] {0, inputs.submitted[1] + 1, inputs.submitted[2] + 1, inputs.submitted[3] + 1}, 0, 2,
                recovered, captured, recordings)) {
                run.follow(expected.size(), run.producers[2], false);
                assertThat(recovered).containsExactlyElementsOf(expected);
                assertBook(recovered.getLast(), 104, BinanceBook.VALID, 5 * UNIT);
                run.send(inputs.observation(2, 1, 1, sbe(105, 6)));
                expected.add(recovered.getLast());
                assertBook(recovered.getLast(), 105, BinanceBook.VALID, 6 * UNIT);
                assertThat(recovered).containsExactlyElementsOf(expected);
                final List<byte[]> raw = replayCaptures(fixture, run.archive, recordings);
                assertThat(raw).containsExactlyElementsOf(captured);
                assertThat(raw).hasSameSizeAs(expected);
                verifyLineage(recovered, captured);
                if (scheduled) { scheduledProfile(run, inputs, third); }
                else if (profile) { profile(run, inputs, third); }
            }
            fixture.assertHealthy();
            assertThat(third.selfOffers.get()).isZero();
        }
    }

    private static byte[] bootstrap() {
        final StringBuilder text = new StringBuilder("{\"lastUpdateId\":100,\"bids\":[");
        for (int side = 0; side < 2; side++) {
            if (side != 0) { text.append("],\"asks\":["); }
            for (int i = 0; i < 5000; i++) {
                if (i != 0) { text.append(','); }
                text.append("[\"").append(side == 0 ? 100_000 - i : 100_001 + i).append("\",\"1\"]");
            }
        }
        text.append("]}");
        final byte[] raw = Arrays.copyOf(MarketCoreTest.utf8(text.toString()), 320 * 1024);
        Arrays.fill(raw, text.length(), raw.length, (byte)' ');
        return raw;
    }

    private static String diff(final long nativeId, final String size) {
        return MarketCoreTest.diff(nativeId, nativeId, "[[\"100000\",\"" + size + "\"]]", "[]");
    }

    private static byte[] sbe(final long nativeId, final long size) {
        final byte[] bytes = MarketCoreTest.sbe(nativeId, nativeId, size);
        new UnsafeBuffer(bytes).putLong(39, 100_000, Wire.ORDER);
        return bytes;
    }

    private static void initialize(final ClusterFixture fixture) {
        fixture.connect();
        final Wire wire = new Wire();
        final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        final byte[] result = fixture.exchange(init, wire.encodeInitialize(init, 0, LOG, BinanceApplication.IDENTITY));
        assertThat(wire.validResult(new UnsafeBuffer(result), 0, result.length)).isTrue();
        assertThat(wire.result.status()).isEqualTo(AdmissionStatus.INITIALIZED);
    }

    private static BookResultDecoder book(final byte[] canonical) {
        final UnsafeBuffer buffer = new UnsafeBuffer(canonical);
        assertThat(new Wire().validEvent(buffer, 0, canonical.length)).isTrue();
        assertThat(buffer.getShort(Wire.EVENT_OVERHEAD + 4, Wire.ORDER) & 65535).isEqualTo(401);
        assertThat(buffer.getShort(Wire.EVENT_OVERHEAD + 6, Wire.ORDER)).isEqualTo((short)1);
        return new BookResultDecoder().wrap(buffer, Wire.EVENT_OVERHEAD + 8, BookResultDecoder.BLOCK_LENGTH, BookResultDecoder.SCHEMA_VERSION);
    }

    private static void assertBook(final byte[] canonical, final long frontier, final int status, final long bidSize) {
        final BookResultDecoder result = book(canonical);
        assertThat(result.frontier()).isEqualTo(frontier);
        assertThat((int)result.status()).isEqualTo(status);
        assertThat(result.bidSize()).isEqualTo(bidSize);
        if (bidSize != 0) {
            assertThat(result.bidPrice()).isEqualTo(100_000 * UNIT);
            assertThat(result.askPrice()).isEqualTo(100_001 * UNIT);
            assertThat(result.askSize()).isEqualTo(UNIT);
            assertThat(result.bidFloor()).isEqualTo(95_001 * UNIT);
            assertThat(result.askCeiling()).isEqualTo(105_000 * UNIT);
        } else {
            assertThat(result.bidPrice()).isZero();
            assertThat(result.askPrice()).isZero();
        }
    }

    private static void verifyLineage(final List<byte[]> canonical, final List<byte[]> captured) {
        for (int i = 0; i < canonical.size(); i++) {
            final BookResultDecoder result = book(canonical.get(i));
            final byte[] original = new byte[result.observationLength()];
            result.getObservation(original, 0, original.length);
            assertThat(original).as("original payload at canonical %s", i + 1)
                .isEqualTo(Arrays.copyOfRange(captured.get(i), Wire.PROPOSAL_OVERHEAD, captured.get(i).length));
        }
    }

    private static byte[] canonical(final byte[] receipt) {
        return Arrays.copyOfRange(receipt, Wire.RESULT_OVERHEAD, receipt.length);
    }

    private static List<byte[]> replayCaptures(final ClusterFixture fixture, final AeronArchive archive,
        final List<CaptureRun> recordings) {
        final List<byte[]> actual = new ArrayList<>();
        for (final CaptureRun recording : recordings) {
            try (Subscription subscription = archive.context().aeron().addSubscription(IPC, 9941)) {
                final long replay = archive.startReplay(recording.id, recording.start, recording.position - recording.start, IPC, 9941);
                try {
                    fixture.await(() -> subscription.imageBySessionId((int)replay) != null, () -> { }, "capture replay image");
                    final Image image = subscription.imageBySessionId((int)replay);
                    final ImageFragmentAssembler assembler = new ImageFragmentAssembler((buffer, offset, length, header) ->
                        actual.add(MarketCoreTest.bytes(buffer, offset, length)), Wire.MAX_PROPOSAL_BYTES);
                    fixture.await(() -> image.position() >= recording.position,
                        () -> image.poll(assembler, 256), "complete raw capture replay");
                    assertThat(assembler.bufferBuilder().limit()).isZero();
                } finally {
                    try { archive.stopReplay(replay); }
                    catch (final ArchiveException error) { if (error.errorCode() != ArchiveException.UNKNOWN_REPLAY) { throw error; } }
                }
            }
        }
        return actual;
    }

    private static final class CaptureRun {
        final long id, start;
        long position;
        CaptureRun(final long id, final long start) { this.id = id; this.start = start; position = start; }
    }

    private static final class Run implements AutoCloseable {
        final ClusterFixture fixture;
        final AeronArchive archive;
        final ReliableProducer[] producers = new ReliableProducer[4];
        final ExclusivePublication capture;
        final CatalogSubscription consumer;
        final CountersReader counters;
        final CaptureRun recording;
        final long registration;
        final int counter;
        final List<byte[]> delivered, captured;
        final Wire wire = new Wire();
        final EventHandler receiver;
        long maxCaptureLagBytes, maxFollowerLag;
        long[] timings;
        int sample;

        Run(final ClusterFixture fixture, final ObservedService service, final long[] next, final long applied,
            final int generation, final List<byte[]> delivered, final List<byte[]> captured,
            final List<CaptureRun> recordings) {
            this.fixture = fixture; this.delivered = delivered; this.captured = captured;
            archive = fixture.connectArchive(8541 + generation);
            fixture.expectClosedConsumerControlResponse(archive);
            for (int i = 1; i <= 3; i++) {
                producers[i] = new ReliableProducer(LOG, 10 + i, next[i]);
                producers[i].connect(fixture.producerContext());
            }
            capture = archive.addRecordedExclusivePublication(CAPTURE_CHANNEL, CAPTURE_STREAM);
            counters = archive.context().aeron().countersReader();
            final int[] found = {CountersReader.NULL_COUNTER_ID};
            final long[] registered = {CountersReader.DEFAULT_REGISTRATION_ID};
            fixture.await(() -> (found[0] = RecordingPos.findCounterIdBySession(counters, capture.sessionId(), archive.archiveId()))
                != CountersReader.NULL_COUNTER_ID
                && (registered[0] = counters.getCounterRegistrationId(found[0])) != CountersReader.DEFAULT_REGISTRATION_ID,
                () -> pollProducers(null, false), "capture recording registration");
            counter = found[0];
            registration = registered[0];
            recording = new CaptureRun(RecordingPos.getRecordingId(counters, counter), archive.getStartPosition(
                RecordingPos.getRecordingId(counters, counter)));
            recordings.add(recording);
            consumer = new CatalogSubscription(archive, LOG, SequencerService.OUTPUT_STREAM, applied, IPC, 9541 + generation);
            receiver = (buffer, offset, length, event) -> {
                if (timings != null) { timings[sample++] = System.nanoTime() - event.readyTimeNanos(); }
                delivered.add(MarketCoreTest.bytes(buffer, offset, length));
            };
        }

        ReliableProducer stage(final UnsafeBuffer proposal) {
            assertThat(wire.validProposal(proposal, 0, proposal.capacity())).isTrue();
            final int source = (int)wire.proposal.sourceId();
            final ReliableProducer sender = producers[source];
            assertThat(sender.submit(source, (int)wire.proposal.sourceConfigVersion(), (int)wire.proposal.clockId(),
                wire.proposal.receiveTimeNanos(), wire.proposal.completeTimeNanos(), wire.proposal.readyTimeNanos(),
                proposal, Wire.PROPOSAL_OVERHEAD, proposal.capacity() - Wire.PROPOSAL_OVERHEAD)).isTrue();
            assertThat(MarketCoreTest.bytes(sender.pendingBuffer(), 0, sender.pendingLength())).isEqualTo(proposal.byteArray());
            // False suppresses every offer/retry until the exact proposal has a complete Archive fence.
            final long[] offered = {0};
            await(() -> (offered[0] = capture.offer(sender.pendingBuffer(), 0, sender.pendingLength())) > 0,
                () -> pump(sender, false), "captured book proposal");
            recording.position = offered[0];
            await(() -> complete(recording.position), () -> pump(sender, false), "complete book capture fence");
            captured.add(proposal.byteArray().clone());
            return sender;
        }

        void send(final UnsafeBuffer proposal) {
            final long target = consumer.sequence() + 1;
            final ReliableProducer sender = stage(proposal);
            await(() -> sender.pendingLength() == 0, () -> pump(sender, true), "book proposal admission receipt");
            assertThat(sender.lastStatus()).isIn(AdmissionStatus.ACCEPTED, AdmissionStatus.DUPLICATE);
            follow(target, sender, false);
        }

        void follow(final long target, final ReliableProducer sender, final boolean allowOffer) {
            await(() -> consumer.sequence() == target, () -> pump(sender, allowOffer), "recorded book follower");
        }

        void pump(final ReliableProducer sender, final boolean offer) {
            pollProducers(sender, offer);
            for (int i = 1; i <= 3; i++) {
                maxFollowerLag = Math.max(maxFollowerLag, producers[i].acknowledgedSequence() - consumer.sequence());
            }
            if (consumer.requiresRefresh()) { consumer.refresh(); } // Cold output-generation qualification after restart.
            consumer.poll(receiver, 256);
        }

        private void pollProducers(final ReliableProducer sender, final boolean offer) {
            for (int i = 1; i <= 3; i++) { producers[i].poll(offer && producers[i] == sender); }
        }

        private boolean complete(final long position) {
            if (counters.getCounterRegistrationId(counter) != registration || !RecordingPos.isActive(counters, counter, recording.id)) {
                return false;
            }
            final long recorded = counters.getCounterValue(counter);
            maxCaptureLagBytes = Math.max(maxCaptureLagBytes, position - recorded);
            return counters.getCounterRegistrationId(counter) == registration
                && RecordingPos.isActive(counters, counter, recording.id) && recorded >= position;
        }

        private void await(final java.util.function.BooleanSupplier done, final Runnable progress, final String description) {
            if (timings == null) { fixture.await(done, progress, description); return; }
            final long deadline = System.nanoTime() + 30_000_000_000L;
            while (!done.getAsBoolean()) {
                progress.run(); fixture.pollAdmin(); fixture.assertHealthy();
                if (System.nanoTime() >= deadline) { throw new IllegalStateException(description); }
                Thread.onSpinWait();
            }
        }

        @Override public void close() {
            consumer.close();
            for (int i = 1; i <= 3; i++) { producers[i].close(); }
            capture.close(); archive.close();
        }
    }

    private static void profile(final Run run, final MarketCoreTest.Inputs inputs, final ObservedService service) throws Exception {
        final int warmup = 1024, measured = 10_000;
        final String transportMode = Boolean.getBoolean("spine.market.busySpin") ? "BusySpin" : "Backoff";
        final Path output = Path.of(System.getProperty("spine.market.profile.output", "build/reports/market-book-integration"))
            .resolve(transportMode + "-" + System.currentTimeMillis() + "-" + System.nanoTime());
        Files.createDirectories(output);
        Files.writeString(output.resolve("attempt.json"), "{\"transportIdleStrategy\":\"" + transportMode +
            "\",\"samples\":" + measured + ",\"warmup\":" + warmup + "}\n");
        long nativeId = 105;
        service.observeReplies = false;
        for (int i = 0; i < warmup; i++) { run.send(inputs.observation(2, 1, 1, sbe(++nativeId, 1 + (i & 1)))); }
        final ThreadMXBean cpu = ManagementFactory.getThreadMXBean();
        assertThat(cpu.isThreadCpuTimeSupported()).isTrue();
        cpu.setThreadCpuTimeEnabled(true);
        final long owner = Thread.currentThread().threadId(), ownerBefore = cpu.getThreadCpuTime(owner);
        final long serviceBefore = cpu.getThreadCpuTime(service.threadId);
        final long processBefore = ProcessHandle.current().info().totalCpuDuration().orElseThrow().toNanos();
        run.maxCaptureLagBytes = run.maxFollowerLag = 0;
        run.timings = new long[measured];
        final long started = System.nanoTime();
        for (int i = 0; i < measured; i++) {
            final UnsafeBuffer proposal = inputs.observation(2, 1, 1, sbe(++nativeId, 1 + (i & 1)));
            final long ready = System.nanoTime();
            proposal.putLong(Wire.HEADER_LENGTH + 36, ready, Wire.ORDER);
            proposal.putLong(Wire.HEADER_LENGTH + 44, ready, Wire.ORDER);
            proposal.putLong(Wire.HEADER_LENGTH + 52, ready, Wire.ORDER);
            run.send(proposal);
        }
        final long elapsed = System.nanoTime() - started;
        final long ownerCpu = cpu.getThreadCpuTime(owner) - ownerBefore;
        final long serviceCpu = cpu.getThreadCpuTime(service.threadId) - serviceBefore;
        final long processCpu = ProcessHandle.current().info().totalCpuDuration().orElseThrow().toNanos() - processBefore;
        assertThat(run.sample).isEqualTo(measured);
        final long[] values = run.timings.clone();
        run.timings = null;
        Arrays.sort(values);
        final String summary = "{\"mode\":\"synthetic closed-loop local IPC book path\",\"samples\":" + measured +
            ",\"transportIdleStrategy\":\"" + transportMode + "\",\"warmup\":" + warmup +
            ",\"levelsPerSide\":5000,\"elapsedNanos\":" + elapsed +
            ",\"readyToRecordedFollowerP50Nanos\":" + values[measured / 2 - 1] +
            ",\"readyToRecordedFollowerP99Nanos\":" + values[measured * 99 / 100 - 1] +
            ",\"readyToRecordedFollowerP999Nanos\":" + values[measured * 999 / 1000 - 1] +
            ",\"readyToRecordedFollowerMaxNanos\":" + values[measured - 1] +
            ",\"captureAndProducerAndConsumerThreadCpuNanos\":" + ownerCpu +
            ",\"serviceAgentThreadCpuNanos\":" + serviceCpu + ",\"processCpuNanos\":" + processCpu +
            ",\"maximumCaptureRecorderLagBytes\":" + run.maxCaptureLagBytes +
            ",\"maximumAcknowledgedToRecordedFollowerLagEvents\":" + run.maxFollowerLag +
            ",\"outputRecorderLagBytes\":null,\"profile\":\"DEDICATED driver; " + transportMode +
            " transport agents; busy application polling; no affinity; fileSyncLevel0; snapshot term32MiB\"," +
            "\"measurementScope\":\"ready before complete capture fence through Cluster book mutation, output Archive and recorded consumer callback; same-process System.nanoTime; one outstanding proposal; test observer allocations; no load or venue latency claim\"}\n";
        Files.writeString(output.resolve("summary.json"), summary);
        final StringBuilder samples = new StringBuilder();
        for (final long timing : values) { samples.append(timing).append('\n'); }
        Files.writeString(output.resolve("latencies-sorted-nanos.txt"), samples);
        System.out.println("market book integration profile " + summary.strip());
    }

    private record Arrival(int head, int source, long dueOffset, UnsafeBuffer proposal) { }

    private static final class Phase {
        final String name;
        final long[] ready = new long[1000], lateness = new long[1000], state = new long[1000];
        final long[] qualified = new long[500], nativeState = new long[500];
        int observations, heads, jsonWinners, sbeWinners;
        long ownerCpu, serviceCpu, processCpu, captureLag, followerLag;
        Phase(final String name) { this.name = name; }
    }

    private static void scheduledProfile(final Run run, final MarketCoreTest.Inputs inputs,
        final ObservedService service) throws Exception {
        final int warmup = 1024, heads = 3000, observations = 5000;
        final String idle = Boolean.getBoolean("spine.market.busySpin") ? "BusySpin" : "Backoff";
        final Path output = Path.of(System.getProperty("spine.market.profile.output", "build/reports/market-book-integration"))
            .resolve("scheduled-" + idle + "-" + System.currentTimeMillis() + "-" + System.nanoTime());
        Files.createDirectories(output);
        Files.writeString(output.resolve("attempt.json"), "{\"workload\":\"scheduled paired JSON/SBE\",\"idle\":\"" + idle +
            "\",\"nativeHeads\":3000,\"observations\":5000,\"scheduledHeadsPerSecond\":1000,\"warmup\":1024}\n");
        service.observeReplies = false;
        long base = 105;
        for (int i = 0; i < warmup; i++) { run.send(inputs.observation(2, 1, 1, sbe(++base, 1 + (i & 1)))); }
        final long[] sourceSequence = {0, inputs.submitted[1], inputs.submitted[2]};
        final List<Arrival> plan = new ArrayList<>(observations);
        for (int head = 1; head <= heads; head++) {
            final int phase = (head - 1) / 500;
            if (phase != 4) { plan.add(new Arrival(head, 1, (head - 1) * 1_000_000L + 200_000,
                inputs.observation(1, 1, 1, MarketCoreTest.utf8(diff(base + head, Integer.toString(1 + (head & 1))))))); }
            if (phase != 2) { plan.add(new Arrival(head, 2, (head - 1) * 1_000_000L + (phase == 1 ? 2_000_000 : 50_000),
                inputs.observation(2, 1, 1, sbe(base + head, 1 + (head & 1))))); }
        }
        plan.sort(Comparator.comparingLong(Arrival::dueOffset)); // Cold; each producer's native order remains increasing.
        assertThat(plan).hasSize(observations);
        final Phase[] phases = {new Phase("baseline"), new Phase("SBE-delayed"), new Phase("SBE-paused"),
            new Phase("SBE-restored"), new Phase("JSON-paused"), new Phase("JSON-restored")};
        final long[] producerHead = new long[3];
        final boolean[] qualified = new boolean[heads + 1];
        final ThreadMXBean cpu = ManagementFactory.getThreadMXBean();
        assertThat(cpu.isThreadCpuTimeSupported()).isTrue(); cpu.setThreadCpuTimeEnabled(true);
        final com.sun.management.OperatingSystemMXBean process =
            (com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        final ObservationProposalEncoder timestamps = new ObservationProposalEncoder();
        final StringBuilder rows = new StringBuilder("phase,head,source,nativeScheduledNanos,arrivalDueNanos,actualReadyNanos,serviceExitNanos,recordedCallbackNanos,firstQualified\n");
        final int capturedStart = run.captured.size(), deliveredStart = run.delivered.size();
        final long canonicalStart = run.consumer.sequence();
        run.timings = new long[observations]; run.sample = 0;
        final long start = System.nanoTime() + 20_000_000;
        while (System.nanoTime() < start) { run.pump(null, false); Thread.onSpinWait(); }
        long ownerLast = cpu.getCurrentThreadCpuTime(), serviceLast = cpu.getThreadCpuTime(service.threadId);
        long processLast = process.getProcessCpuTime(), frontier = base;
        assertThat(ownerLast).isNotNegative(); assertThat(serviceLast).isNotNegative(); assertThat(processLast).isNotNegative();
        for (final Arrival arrival : plan) {
            final Phase phase = phases[(arrival.head - 1) / 500];
            assertThat(arrival.head).isGreaterThan((int)producerHead[arrival.source]);
            producerHead[arrival.source] = arrival.head;
            final long nativeDue = start + (arrival.head - 1) * 1_000_000L, due = start + arrival.dueOffset;
            while (System.nanoTime() < due) { run.pump(null, false); Thread.onSpinWait(); }
            final long ready = System.nanoTime();
            timestamps.wrap(arrival.proposal, Wire.HEADER_LENGTH)
                .receiveTimeNanos(ready).completeTimeNanos(ready).readyTimeNanos(ready);
            assertThat(run.wire.validProposal(arrival.proposal, 0, arrival.proposal.capacity())).isTrue();
            assertThat(run.wire.proposal.producerId()).isEqualTo(10 + arrival.source);
            assertThat(run.wire.proposal.producerSequence()).isEqualTo(++sourceSequence[arrival.source]);
            service.measuredProducer = run.wire.proposal.producerId(); service.measuredSequence = run.wire.proposal.producerSequence();
            service.callbackExitNanos = 0;
            run.maxCaptureLagBytes = run.maxFollowerLag = 0;
            run.send(arrival.proposal);
            run.await(() -> service.callbackExitNanos != 0, () -> run.pump(null, false), "observed service callback exit");
            final long delivered = ready + run.timings[run.sample - 1], exit = service.callbackExitNanos;
            assertThat(ready).isGreaterThanOrEqualTo(due);
            assertThat(delivered).isGreaterThanOrEqualTo(ready);
            assertThat(exit).isGreaterThanOrEqualTo(ready); // The recorded callback may race ahead of this observer.
            final long expected = Math.max(frontier, base + arrival.head);
            assertBook(run.delivered.getLast(), expected, BinanceBook.VALID, (1 + ((expected - base) & 1)) * UNIT);
            final boolean first = expected > frontier;
            assertThat((int)book(run.delivered.getLast()).outcome()).isEqualTo(first ? BinanceBook.APPLIED : BinanceBook.STALE);
            final UnsafeBuffer canonical = new UnsafeBuffer(run.delivered.getLast());
            assertThat(run.wire.validEvent(canonical, 0, canonical.capacity())).isTrue();
            assertThat(run.wire.event.sequence()).isEqualTo(canonicalStart + run.sample);
            assertThat(run.wire.event.sourceId()).isEqualTo(arrival.source);
            assertThat(run.wire.event.sourceConfigVersion()).isEqualTo(1);
            assertThat(run.wire.event.clockId()).isEqualTo(1);
            assertThat(run.wire.event.receiveTimeNanos()).isEqualTo(ready);
            assertThat(run.wire.event.completeTimeNanos()).isEqualTo(ready);
            assertThat(run.wire.event.readyTimeNanos()).isEqualTo(ready);
            if (first) {
                assertThat(expected).isEqualTo(frontier + 1);
                assertThat(qualified[arrival.head]).isFalse(); qualified[arrival.head] = true;
                phase.qualified[phase.heads] = delivered - nativeDue; phase.nativeState[phase.heads++] = exit - nativeDue;
                if (arrival.source == 1) { phase.jsonWinners++; } else { phase.sbeWinners++; }
            }
            frontier = expected;
            final int sample = phase.observations++;
            phase.ready[sample] = delivered - ready; phase.lateness[sample] = ready - due; phase.state[sample] = exit - ready;
            phase.captureLag = Math.max(phase.captureLag, run.maxCaptureLagBytes);
            phase.followerLag = Math.max(phase.followerLag, run.maxFollowerLag);
            rows.append(phase.name).append(',').append(arrival.head).append(',').append(arrival.source).append(',')
                .append(nativeDue).append(',').append(due).append(',').append(ready).append(',').append(exit).append(',')
                .append(delivered).append(',').append(first).append('\n');
            final long ownerNow = cpu.getCurrentThreadCpuTime(), serviceNow = cpu.getThreadCpuTime(service.threadId);
            final long processNow = process.getProcessCpuTime();
            phase.ownerCpu += ownerNow - ownerLast; phase.serviceCpu += serviceNow - serviceLast; phase.processCpu += processNow - processLast;
            ownerLast = ownerNow; serviceLast = serviceNow; processLast = processNow;
        }
        final long elapsed = System.nanoTime() - start;
        run.timings = null; service.measuredSequence = 0;
        assertThat(run.sample).isEqualTo(observations);
        assertThat(frontier).isEqualTo(base + heads);
        assertThat(run.captured.size() - capturedStart).isEqualTo(observations);
        assertThat(run.delivered.size() - deliveredStart).isEqualTo(observations);
        verifyLineage(run.delivered.subList(deliveredStart, run.delivered.size()),
            run.captured.subList(capturedStart, run.captured.size()));
        for (int i = 1; i <= heads; i++) { assertThat(qualified[i]).as("qualified native head %s", i).isTrue(); }
        final StringBuilder summary = new StringBuilder("{\"workload\":\"fixed scheduled paired JSON/SBE\",\"transportIdleStrategy\":\"")
            .append(idle).append("\",\"nativeHeads\":3000,\"observations\":5000,\"headPeriodNanos\":1000000,\"JSONDelayNanos\":200000,\"SBEBaseDelayNanos\":50000,\"SBEDegradedDelayNanos\":2000000,\"warmup\":1024,\"levelsPerSide\":5000,\"nativeBase\":")
            .append(base).append(",\"scheduleStartNanos\":").append(start).append(",\"elapsedNanos\":").append(elapsed)
            .append(",\"achievedHeadsPerSecond\":").append(heads * 1_000_000_000.0 / elapsed)
            .append(",\"achievedObservationsPerSecond\":").append(observations * 1_000_000_000.0 / elapsed).append(",\"phases\":[");
        for (int i = 0; i < phases.length; i++) {
            final Phase p = phases[i];
            assertThat(p.observations).isEqualTo(i == 2 || i == 4 ? 500 : 1000);
            assertThat(p.heads).isEqualTo(500);
            assertThat(p.jsonWinners).isEqualTo(i == 1 || i == 2 ? 500 : 0);
            assertThat(p.sbeWinners).isEqualTo(i == 1 || i == 2 ? 0 : 500);
            if (i != 0) { summary.append(','); }
            summary.append("{\"phase\":\"").append(p.name).append("\",\"observations\":").append(p.observations)
                .append(",\"firstPlannedHead\":").append(i * 500 + 1).append(",\"lastPlannedHead\":").append((i + 1) * 500)
                .append(",\"qualifiedHeads\":").append(p.heads).append(",\"JSONFirstWinners\":").append(p.jsonWinners)
                .append(",\"SBEFirstWinners\":").append(p.sbeWinners).append(",\"nativeToQualifiedRecordedNanos\":").append(distribution(p.qualified, p.heads))
                .append(",\"readyToRecordedNanos\":").append(distribution(p.ready, p.observations))
                .append(",\"scheduleLatenessNanos\":").append(distribution(p.lateness, p.observations))
                .append(",\"readyToServiceExitUpperBoundNanos\":").append(distribution(p.state, p.observations))
                .append(",\"nativeToQualifiedServiceExitUpperBoundNanos\":").append(distribution(p.nativeState, p.heads))
                .append(",\"testOwnerCpuNanos\":").append(p.ownerCpu).append(",\"serviceAgentCpuNanos\":").append(p.serviceCpu)
                .append(",\"processCpuNanos\":").append(p.processCpu).append(",\"sampledCaptureLagBytes\":").append(p.captureLag)
                .append(",\"sampledAcknowledgedToFollowerLagEvents\":").append(p.followerLag).append('}');
        }
        summary.append("],\"outputRecorderLagBytes\":null,\"scope\":\"same-process System.nanoTime; fixed schedule never rebased; one outstanding captured proposal; preencoded frames have receive/complete/ready stamped equally at actual arrival; deliberate source pauses omit observations; all 5000 observations verified and retained as u32 big-endian length followed by exact wire bytes; service callback exit includes capture, Cluster commit, book mutation and output publication and is an upper bound for state application; phase CPU deltas follow arrival attribution by native head including idle polling and test assertions; sync0, no affinity; synthetic fixture and observer overhead; availability and ordering under synthetic delay/pause, with no single-source tail comparator, actual WebSocket disconnect, saturation or venue latency claim; per-phase 500-head p999 is the sample maximum\"}\n");
        Files.writeString(output.resolve("summary.json"), summary);
        Files.writeString(output.resolve("samples.csv"), rows);
        writeFrames(output.resolve("captured-proposals.u32be-frames"), run.captured, capturedStart);
        writeFrames(output.resolve("recorded-canonical.u32be-frames"), run.delivered, deliveredStart);
        System.out.println("market scheduled profile " + output + " " + summary.toString().strip());
    }

    private static String distribution(final long[] samples, final int count) {
        final long[] sorted = Arrays.copyOf(samples, count); Arrays.sort(sorted);
        return "{\"count\":" + count + ",\"p50\":" + sorted[(count - 1) / 2] + ",\"p99\":" + sorted[(count * 99 - 1) / 100] +
            ",\"p999\":" + sorted[(count * 999 - 1) / 1000] + ",\"max\":" + sorted[count - 1] + "}";
    }

    private static void writeFrames(final Path file, final List<byte[]> frames, final int start) throws Exception {
        try (DataOutputStream output = new DataOutputStream(Files.newOutputStream(file))) {
            for (int i = start; i < frames.size(); i++) { output.writeInt(frames.get(i).length); output.write(frames.get(i)); }
        }
    }

    private static final class ObservedService implements ClusteredService {
        final SequencerService delegate = new SequencerService(BinanceApplication.IDENTITY, new BinanceApplication());
        final Wire wire = new Wire();
        final AtomicInteger selfOffers = new AtomicInteger();
        volatile boolean restored, hold, observeReplies = true;
        volatile long holdProducer, holdSequence, threadId;
        volatile long measuredProducer, measuredSequence, callbackExitNanos;
        volatile byte[] heldReceipt, retryReceipt;

        @Override public void onStart(final Cluster cluster, final Image snapshot) {
            restored = snapshot != null;
            final Cluster observed = (Cluster)Proxy.newProxyInstance(Cluster.class.getClassLoader(), new Class<?>[] {Cluster.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("offer")) { selfOffers.incrementAndGet(); throw new AssertionError("second Cluster commit"); }
                    try { return method.invoke(cluster, arguments); }
                    catch (InvocationTargetException failure) { throw failure.getCause(); }
                });
            delegate.onStart(observed, snapshot);
        }

        @Override public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
            final int offset, final int length, final Header header) {
            threadId = Thread.currentThread().threadId();
            final boolean timed = measuredSequence != 0 && wire.validProposal(buffer, offset, length)
                && wire.proposal.producerId() == measuredProducer && wire.proposal.producerSequence() == measuredSequence;
            ClientSession response = session;
            if (session != null && observeReplies) {
                response = (ClientSession)Proxy.newProxyInstance(ClientSession.class.getClassLoader(), new Class<?>[] {ClientSession.class},
                    (proxy, method, arguments) -> {
                        if (method.getName().equals("offer") && arguments.length == 3) {
                            final byte[] bytes = MarketCoreTest.bytes((DirectBuffer)arguments[0], (int)arguments[1], (int)arguments[2]);
                            if (wire.validResult(new UnsafeBuffer(bytes), 0, bytes.length) && wire.result.producerId() == holdProducer
                                && wire.result.producerSequence() == holdSequence) {
                                if (wire.result.status() == AdmissionStatus.ACCEPTED && heldReceipt == null) { heldReceipt = bytes; }
                                if (wire.result.status() == AdmissionStatus.DUPLICATE) { retryReceipt = bytes; }
                                if (hold) { return Publication.BACK_PRESSURED; }
                            }
                        }
                        try { return method.invoke(session, arguments); }
                        catch (InvocationTargetException failure) { throw failure.getCause(); }
                    });
            }
            delegate.onSessionMessage(response, timestamp, buffer, offset, length, header);
            if (timed && callbackExitNanos == 0) { callbackExitNanos = System.nanoTime(); }
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
