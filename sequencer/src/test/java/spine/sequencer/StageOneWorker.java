package spine.sequencer;

import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.AeronCluster;
import spine.client.CatalogSubscription;
import spine.client.ReliableProducer;
import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import spine.protocol.sbe.AdmissionStatus;
import spine.protocol.sbe.SequencedEventDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/** Child JVM. Its collector records exact input before admission; application/control fixtures are test-owned. */
public final class StageOneWorker {
    static final long LOG = 42;
    static final String IPC = "aeron:ipc?term-length=1m";
    static final int DATA_STREAM = SequencerService.OUTPUT_STREAM;
    static final int CAPTURE_STREAM = 15_001;
    static final long TIMEOUT = TimeUnit.SECONDS.toNanos(30);
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    private static long nextSample;
    private static long lastBegin;

    private StageOneWorker() { }

    public static void main(final String[] args) throws Exception {
        if (args[0].equals("producer")) {
            producer(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        } else if (args[0].equals("consumer")) {
            consumer(args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]),
                Path.of(args[5]), Boolean.parseBoolean(args[6]));
        } else { throw new IllegalArgumentException("unknown worker role"); }
    }

    private static AeronArchive connectArchive(final String driver, final int request, final int response) {
        return AeronArchive.connect(new AeronArchive.Context().aeronDirectoryName(driver)
            .controlRequestChannel(IPC).controlRequestStreamId(request).controlResponseChannel(IPC)
            .controlResponseStreamId(response).messageTimeoutNs(TIMEOUT));
    }

    private static void producer(final String driver, final int request, final int response) throws Exception {
        final BufferedReader commands = new BufferedReader(new InputStreamReader(System.in));
        try (AeronArchive archive = connectArchive(driver, request, response)) {
            control(archive);
            final Captured captured = restoreCapture(archive, response + 100);
            try (Journal journal = new Journal(archive);
                 ReliableProducer a = new ReliableProducer(LOG, 10, captured.next[0]);
                 ReliableProducer b = new ReliableProducer(LOG, 11, captured.next[1])) {
                final ReliableProducer[] producers = {a, b};
                final Wire receipts = new Wire();
                final boolean[] discard = {false};
                final boolean[] discarded = {false};
                final long[] lastCanonical = {0};
                try (AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context().aeronDirectoryName(driver)
                    .ingressChannel(IPC).egressChannel(IPC).messageTimeoutNs(TIMEOUT)
                    .egressListener((session, time, buffer, offset, length, header) -> {
                        if (!receipts.validResult(buffer, offset, length)) { throw new IllegalStateException("malformed actual receipt"); }
                        final int source = (int)receipts.result.producerId() - 10;
                        if (source < 0 || source > 1) { throw new IllegalStateException("foreign receipt"); }
                        if (receipts.result.producerSequence() < producers[source].nextSequence()) { return; }
                        if (discard[0]) {
                            if (receipts.result.status() != AdmissionStatus.ACCEPTED || source != 0) {
                                throw new IllegalStateException("loss fixture requires actual ACCEPTED receipt");
                            }
                            lastCanonical[0] = receipts.event.sequence();
                            discarded[0] = true;
                            return;
                        }
                        journal.record(buffer, offset, length);
                        producers[source].onReceipt(buffer, offset, length);
                        if (producers[source].failureReason() != null) { throw new IllegalStateException("producer receipt rejected", producers[source].failureReason()); }
                    }))) {
                    emit("IDENTITIES 10 11");
                    for (int i = 0; i < 2; i++) {
                        if (captured.pending[i] != null) {
                            final UnsafeBuffer exact = new UnsafeBuffer(captured.pending[i]);
                            require(producers[i].restoreExact(exact, 0, exact.capacity()), "exact captured proposal restore");
                            emit("PENDING " + digest(exact, 0, exact.capacity()));
                            submit(cluster, producers[i]);
                            require(producers[i].lastStatus() == AdmissionStatus.DUPLICATE, "lost receipt must recover DUPLICATE");
                            emit("RECOVERED " + producers[i].acknowledgedSequence());
                        }
                    }
                    emit("READY producer");
                    while (true) {
                        while (!commands.ready()) { cluster.pollEgress(); cluster.sendKeepAlive(); sampleIfDue(); LockSupport.parkNanos(100_000); }
                        final String line = commands.readLine();
                        if (line == null) { return; }
                        final String[] command = line.split(" ");
                        switch (command[0]) {
                            case "STOP" -> { sample(); return; }
                            case "DEFINE" -> {
                                for (int source = 1; source <= 2; source++) {
                                    final UnsafeBuffer payload = new UnsafeBuffer(new byte[4096]);
                                    final int length = new CaptureCodec().encodeDefinition(payload, 0, -1, labels(source));
                                    publish(cluster, producers[source - 1], journal, source, payload, length, -source, commands);
                                }
                                emit("DONE DEFINE");
                            }
                            case "CAPTURE" -> {
                                final int from = Integer.parseInt(command[1]);
                                final int through = Integer.parseInt(command[2]);
                                final int bytes = Integer.parseInt(command[3]);
                                final long period = Long.parseLong(command[4]);
                                final long start = System.nanoTime();
                                long lateness = 0;
                                for (int observation = from; observation <= through; observation++) {
                                    final long due = start + (observation - from) * period;
                                    while (System.nanoTime() < due) { cluster.pollEgress(); LockSupport.parkNanos(100_000); }
                                    final int source = (observation & 1) == 1 ? 1 : 2;
                                    final UnsafeBuffer payload = capture(observation, bytes);
                                    publish(cluster, producers[source - 1], journal, source, payload, bytes, observation, commands);
                                    lateness = Math.max(lateness, lastBegin - due);
                                    sampleIfDue();
                                }
                                emit("DONE CAPTURE " + through + " " + (System.nanoTime() - start) + " " + lateness);
                            }
                            case "LOSS" -> {
                                final int observation = Integer.parseInt(command[1]);
                                require((observation & 1) == 1, "loss requires source a");
                                final UnsafeBuffer payload = capture(observation, Wire.MAX_PAYLOAD_BYTES);
                                require(a.submit(1, 1, 1, -1, -1, System.currentTimeMillis() * 1_000_000,
                                    payload, 0, payload.capacity()), "prepare loss");
                                journal.record(a.pendingBuffer(), 0, a.pendingLength());
                                emit("PERSISTED " + digest(a.pendingBuffer(), 0, a.pendingLength()) + " 10 " + a.nextSequence());
                                begin(observation, commands);
                                discard[0] = true;
                                await(() -> cluster.offer(a.pendingBuffer(), 0, a.pendingLength()) > 0, cluster::pollEgress);
                                await(() -> discarded[0], cluster::pollEgress);
                                emit("LOST " + lastCanonical[0]);
                                while (true) { cluster.pollEgress(); cluster.sendKeepAlive(); sampleIfDue(); LockSupport.parkNanos(1_000_000); }
                            }
                            default -> throw new IllegalArgumentException("unknown producer command " + line);
                        }
                    }
                }
            }
        }
    }

    private static void publish(final AeronCluster cluster, final ReliableProducer producer, final Journal journal,
        final int source, final UnsafeBuffer payload, final int length, final int observation, final BufferedReader commands) throws Exception {
        begin(observation, commands);
        final long before = System.nanoTime();
        require(producer.submit(source, 1, 1, -1, -1, System.currentTimeMillis() * 1_000_000,
            payload, 0, length), "prepare capture");
        journal.record(producer.pendingBuffer(), 0, producer.pendingLength());
        submit(cluster, producer);
        emit("ACK " + observation + " " + producer.acknowledgedSequence() + " " + (System.nanoTime() - before));
    }

    private static void submit(final AeronCluster cluster, final ReliableProducer producer) {
        final long deadline = System.nanoTime() + TIMEOUT;
        long nextOffer = 0;
        long keepAlive = 0;
        while (producer.pendingLength() != 0) {
            cluster.pollEgress();
            if (producer.pendingLength() == 0) { return; }
            final long now = System.nanoTime();
            if (now >= nextOffer && cluster.offer(producer.pendingBuffer(), 0, producer.pendingLength()) > 0) {
                nextOffer = now + ReliableProducer.RETRY_INTERVAL_NS;
            }
            if (now >= keepAlive && cluster.sendKeepAlive()) { keepAlive = now + 500_000_000; }
            if (now >= deadline) { throw new IllegalStateException("captured proposal admission timeout"); }
            LockSupport.parkNanos(100_000);
        }
    }

    private static void begin(final int observation, final BufferedReader commands) throws Exception {
        emit("BEGIN " + observation);
        require(("GO " + observation).equals(commands.readLine()), "parent admission handshake");
        lastBegin = System.nanoTime();
    }

    static byte[][] labels(final int source) {
        final String[] values = {"fixture-provider-" + source, "fixture:BTC:trades", "fixture-session-1",
            "stage-one-worker", "synthetic binary venue events; numeric native and frame identity; capture-v1",
            "UTC nanoseconds; receive/complete unavailable; no external sync claim"};
        return Arrays.stream(values).map(value -> value.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
    }

    private static UnsafeBuffer capture(final int observation, final int totalLength) {
        final long nativeId = (observation + 1L) / 2;
        final UnsafeBuffer raw = new UnsafeBuffer(new byte[totalLength - CaptureCodec.OBSERVATION_OVERHEAD]);
        raw.putLong(0, nativeId, ORDER);
        for (int i = 8; i < raw.capacity(); i++) { raw.putByte(i, (byte)(nativeId + i)); }
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[totalLength]);
        require(new CaptureCodec().encodeObservation(payload, 0, observation, 0, nativeId,
            null, 0, 0, raw, 0, raw.capacity()) == totalLength, "capture size");
        return payload;
    }

    private static void consumer(final String driver, final int request, final int response, final int replay,
        final Path checkpoint, final boolean restore) throws Exception {
        final Projection projection = new Projection();
        if (restore) { projection.restore(Files.readAllBytes(checkpoint)); }
        final ConcurrentLinkedQueue<String> commands = new ConcurrentLinkedQueue<>();
        final Thread reader = new Thread(() -> {
            try {
                final BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
                for (String line; (line = input.readLine()) != null;) { commands.add(line); }
                commands.add("STOP");
            } catch (final Exception error) { commands.add("FAIL " + error); }
        }, "stage-one-control");
        reader.setDaemon(true); reader.start();
        try (AeronArchive archive = connectArchive(driver, request, response);
             CatalogSubscription data = new CatalogSubscription(archive, LOG, DATA_STREAM, projection.count, IPC, replay)) {
            control(archive);
            emit("READY consumer");
            boolean paused = false;
            long fence = -1;
            long head = projection.count;
            long recoveryFrom = 0;
            long recoveryStart = 0;
            while (true) {
                for (String line; (line = commands.poll()) != null;) {
                    final String[] command = line.split(" ");
                    switch (command[0]) {
                        case "STOP" -> { report(projection, data, head); sample(); return; }
                        case "PAUSE" -> { paused = true; emit("PAUSED " + data.sequence()); }
                        case "RESUME" -> { paused = false; recoveryFrom = data.sequence(); recoveryStart = System.nanoTime(); }
                        case "HEAD" -> {
                            head = Long.parseLong(command[1]);
                            if (paused && head > data.sequence()) { emit("BEHIND " + data.sequence() + " " + head + " " + data.status()); }
                        }
                        case "FENCE" -> fence = Long.parseLong(command[1]);
                        case "CHECKPOINT" -> { Files.write(checkpoint, projection.prefix.toByteArray()); emit("CHECKPOINT " + data.sequence()); }
                        case "REFRESH" -> data.refresh();
                        case "REPORT" -> report(projection, data, head);
                        default -> throw new IllegalArgumentException("unknown consumer command " + line);
                    }
                }
                if (!paused) {
                    data.poll(projection::apply, recoveryStart == 0 ? 256 : 64);
                }
                if (recoveryStart != 0 && data.sequence() >= head) {
                    emit("RECOVERY " + recoveryFrom + " " + data.sequence() + " " + (System.nanoTime() - recoveryStart) + " true");
                    recoveryStart = 0;
                }
                if (fence >= 0 && data.sequence() == fence) { report(projection, data, head); fence = -1; }
                sampleIfDue();
                LockSupport.parkNanos(100_000);
            }
        }
    }

    private static void report(final Projection projection, final CatalogSubscription data, final long head) throws Exception {
        final MessageDigest clone = (MessageDigest)projection.digest.clone();
        emit("REPORT " + projection.count + " " + projection.sum + " " + projection.observations + " " + projection.pairs
            + " " + HexFormat.of().formatHex(clone.digest()) + " " + data.sequence() + " " + data.status()
            + " " + (data.sequence() == head ? "AT_HEAD" : "BEHIND") + " true");
    }

    private static final class Projection {
        final Wire wire = new Wire();
        final CaptureCodec capture = new CaptureCodec();
        final MessageDigest digest;
        final MessageDigest rawDigest;
        final ByteArrayOutputStream prefix = new ByteArrayOutputStream();
        final byte[] scratch = new byte[Wire.MAX_EVENT_BYTES];
        final byte[] size = new byte[4];
        final UnsafeBuffer sizeBuffer = new UnsafeBuffer(size);
        final byte[] hash = new byte[32];
        final byte[] firstHash = new byte[32];
        final boolean[] definitions = new boolean[3];
        long count, observations, pairs, sum, firstNative;
        boolean restoring;

        Projection() throws Exception { digest = MessageDigest.getInstance("SHA-256"); rawDigest = MessageDigest.getInstance("SHA-256"); }

        void apply(final DirectBuffer buffer, final int offset, final int length, final SequencedEventDecoder event) {
            try {
                require(event.logId() == LOG && event.sequence() == count + 1, "projection identity/order");
                require(event.sourceConfigVersion() == 1 && event.clockId() == 1 && event.receiveTimeNanos() == -1
                    && event.completeTimeNanos() == -1 && event.readyTimeNanos() >= 0 && event.sequencedTimeNanos() >= 0,
                    "clock/configuration lineage");
                final int source = (int)event.sourceId();
                require(source == 1 || source == 2, "source identity");
                final int payloadOffset = offset + Wire.EVENT_OVERHEAD;
                final int payloadLength = length - Wire.EVENT_OVERHEAD;
                if (capture.templateId(buffer, payloadOffset, payloadLength) == 1) {
                    require(capture.validDefinition(buffer, payloadOffset, payloadLength) && !definitions[source], "source definition");
                    final byte[][] expected = labels(source);
                    for (int i = 0; i < expected.length; i++) {
                        require(capture.labelLength(i) == expected[i].length, "definition label length");
                        for (int j = 0; j < expected[i].length; j++) {
                            require(buffer.getByte(capture.labelOffset(i) + j) == expected[i][j], "definition label bytes");
                        }
                    }
                    definitions[source] = true;
                } else {
                    require(definitions[source] && capture.validObservation(buffer, payloadOffset, payloadLength), "capture observation");
                    final long ordinal = observations + 1;
                    final long nativeId = (ordinal + 1) / 2;
                    require(capture.observation.nativeNumericId() == nativeId && capture.observation.rawFrameSequence() == ordinal
                        && capture.observation.subeventIndex() == 0 && capture.nativeLength == 0
                        && source == ((ordinal & 1) == 1 ? 1 : 2), "native/frame/source lineage");
                    require(buffer.getLong(capture.rawOffset, ORDER) == nativeId, "raw native identity");
                    for (int i = 8; i < capture.rawLength; i++) { require(buffer.getByte(capture.rawOffset + i) == (byte)(nativeId + i), "raw fixture bytes"); }
                    buffer.getBytes(capture.rawOffset, scratch, 0, capture.rawLength);
                    rawDigest.update(scratch, 0, capture.rawLength);
                    rawDigest.digest(hash, 0, 32);
                    if (source == 1) { firstNative = nativeId; System.arraycopy(hash, 0, firstHash, 0, 32); }
                    else { require(firstNative == nativeId && Arrays.equals(firstHash, hash), "same native event provider copies"); pairs++; }
                    observations++;
                    sum += nativeId;
                }
                buffer.getBytes(offset, scratch, 0, length);
                sizeBuffer.putInt(0, length, ORDER);
                digest.update(size); digest.update(scratch, 0, length);
                if (count < 22 && !restoring) { prefix.write(size); prefix.write(scratch, 0, length); }
                count++;
                if (!restoring) { emit("APPLIED " + count); }
            } catch (final Exception error) { throw new IllegalStateException("projection failed", error); }
        }

        void restore(final byte[] bytes) {
            final UnsafeBuffer state = new UnsafeBuffer(bytes);
            restoring = true;
            int offset = 0;
            while (offset < bytes.length) {
                require(offset <= bytes.length - 4, "checkpoint frame length");
                final int length = state.getInt(offset, ORDER);
                offset += 4;
                require(wire.validEvent(state, offset, length), "checkpoint canonical frame");
                apply(state, offset, length, wire.event);
                offset += length;
            }
            require(offset == bytes.length && count == 22, "checkpoint exact prefix");
            restoring = false;
            prefix.writeBytes(bytes);
        }
    }

    private static final class Captured {
        final long[] next = {1, 1};
        final byte[][] pending = new byte[2][];
        final Wire wire = new Wire();

        void apply(final DirectBuffer buffer, final int offset, final int length) {
            if (wire.validProposal(buffer, offset, length)) {
                final int source = (int)wire.proposal.producerId() - 10;
                require(source >= 0 && source < 2 && wire.proposal.logId() == LOG
                    && wire.proposal.producerSequence() == next[source] && pending[source] == null, "capture proposal continuity");
                pending[source] = new byte[length];
                buffer.getBytes(offset, pending[source]);
            } else {
                require(wire.validResult(buffer, offset, length), "capture journal receipt");
                final int source = (int)wire.result.producerId() - 10;
                require(source >= 0 && source < 2, "capture receipt identity");
                if (wire.result.producerSequence() < next[source]) { return; }
                require(pending[source] != null && wire.result.producerSequence() == next[source]
                    && (wire.result.status() == AdmissionStatus.ACCEPTED || wire.result.status() == AdmissionStatus.DUPLICATE),
                    "capture journal admission");
                next[source]++;
                pending[source] = null;
            }
        }
    }

    private static Captured restoreCapture(final AeronArchive archive, final int replayStream) {
        final List<long[]> recordings = new ArrayList<>();
        archive.listRecordingsForUri(0, 1_000, "spine-capture", CAPTURE_STREAM,
            (control, correlation, id, startTime, stopTime, start, stop, termId, segment, term, mtu, session, stream, stripped, original, identity) ->
                recordings.add(new long[] {id, start, stop}));
        final Captured state = new Captured();
        for (final long[] recording : recordings) {
            long end = recording[2];
            final long deadline = System.nanoTime() + TIMEOUT;
            while (end == AeronArchive.NULL_POSITION) {
                end = archive.getStopPosition(recording[0]);
                if (System.nanoTime() >= deadline) { throw new IllegalStateException("previous capture writer still owns journal"); }
                LockSupport.parkNanos(100_000);
            }
            if (end == recording[1]) { continue; }
            try (Subscription subscription = archive.context().aeron().addSubscription(IPC, replayStream)) {
                final long replay = archive.startReplay(recording[0], recording[1], end - recording[1], IPC, replayStream);
                final Throwable[] failure = {null};
                final ImageFragmentAssembler assembler = new ImageFragmentAssembler((buffer, offset, length, header) -> {
                    if (failure[0] != null) { return; }
                    try { state.apply(buffer, offset, length); }
                    catch (final RuntimeException | Error error) { failure[0] = error; }
                }, Wire.MAX_RESULT_BYTES);
                Image image = null;
                while (image == null) {
                    image = subscription.imageBySessionId((int)replay);
                    if (System.nanoTime() >= deadline) { throw new IllegalStateException("capture replay image timeout"); }
                    LockSupport.parkNanos(100_000);
                }
                while (image.position() < end) {
                    image.poll((buffer, offset, length, header) -> {
                        if (failure[0] != null) { return; }
                        try { assembler.onFragment(buffer, offset, length, header); }
                        catch (final RuntimeException | Error error) { failure[0] = error; }
                    }, 256);
                    if (failure[0] != null) { throw new IllegalStateException("invalid captured history", failure[0]); }
                    if (image.isClosed() && image.position() < end) { throw new IllegalStateException("capture replay ended before fence"); }
                    if (System.nanoTime() >= deadline) { throw new IllegalStateException("capture replay fence timeout"); }
                    LockSupport.parkNanos(100_000);
                }
                require(assembler.bufferBuilder().limit() == 0, "partial captured message at closed fence");
            }
        }
        return state;
    }

    private static final class Journal implements AutoCloseable {
        final AeronArchive archive;
        final ExclusivePublication publication;
        final CountersReader counters;
        final int counter;
        final long recording;

        Journal(final AeronArchive archive) {
            this.archive = archive;
            publication = archive.addRecordedExclusivePublication(IPC + "|alias=spine-capture", CAPTURE_STREAM);
            counters = archive.context().aeron().countersReader();
            final int[] found = {CountersReader.NULL_COUNTER_ID};
            await(() -> (found[0] = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId())) != CountersReader.NULL_COUNTER_ID,
                () -> { });
            counter = found[0];
            recording = RecordingPos.getRecordingId(counters, counter);
        }

        void record(final DirectBuffer buffer, final int offset, final int length) {
            final long[] position = {0};
            await(() -> (position[0] = publication.offer(buffer, offset, length)) > 0, () -> { });
            await(() -> RecordingPos.isActive(counters, counter, recording) && counters.getCounterValue(counter) >= position[0], () -> { });
        }

        @Override public void close() { publication.close(); }
    }

    private static String digest(final DirectBuffer buffer, final int offset, final int length) throws Exception {
        final byte[] bytes = new byte[length]; buffer.getBytes(offset, bytes);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void control(final AeronArchive archive) {
        final String session = ChannelUri.parse(archive.context().controlResponseChannel()).get("session-id");
        emit("CONTROL " + archive.context().controlResponseStreamId() + " " + archive.controlSessionId() + " " + (session == null ? "none" : session));
    }

    private static void await(final BooleanSupplier done, final Runnable work) {
        final long deadline = System.nanoTime() + TIMEOUT;
        while (!done.getAsBoolean()) {
            work.run();
            if (System.nanoTime() >= deadline) { throw new IllegalStateException("worker transport timeout"); }
            LockSupport.parkNanos(100_000);
        }
    }

    private static void require(final boolean condition, final String message) { if (!condition) { throw new IllegalStateException(message); } }
    private static void sampleIfDue() { if (System.nanoTime() >= nextSample) { sample(); nextSample = System.nanoTime() + 100_000_000; } }
    private static void sample() {
        emit("SAMPLE " + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() + " "
            + ProcessHandle.current().info().totalCpuDuration().orElseThrow().toNanos() + " "
            + ManagementFactory.getRuntimeMXBean().getUptime() * 1_000_000L);
    }
    private static void emit(final String line) { System.out.println(line); System.out.flush(); }
}
