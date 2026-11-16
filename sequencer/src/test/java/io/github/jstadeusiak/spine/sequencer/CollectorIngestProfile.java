package io.github.jstadeusiak.spine.sequencer;

import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.client.CatalogSubscription;
import io.github.jstadeusiak.spine.client.EventHandler;
import io.github.jstadeusiak.spine.client.RecordingCatalog;
import io.github.jstadeusiak.spine.feeds.CapturedIngress;
import io.github.jstadeusiak.spine.feeds.WebSocketCollector;
import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import io.github.jstadeusiak.spine.protocol.sbe.SequencedEventDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/** Fixed local ingestion diagnostic; peer scheduling and JDK networking are not allocation/venue latency benchmarks. */
public final class CollectorIngestProfile {
    private static final int RATE = 150, WARMUP = 300, BASELINE = 600, BURST = 128, TRADES = WARMUP + BASELINE + BURST;
    private static final long SECOND = 1_000_000_000L, DRAIN = 30 * SECOND;
    private static final String IPC = "aeron:ipc?term-length=1m";
    private static final byte[] ACK = "{\"result\":null,\"id\":1}".getBytes(StandardCharsets.UTF_8);
    private static final String SUBSCRIBE = "{\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@trade\"],\"id\":1}";

    public static void main(final String[] args) throws Exception {
        if (args.length < 1 || args.length > 2 || args.length == 2 && !args[1].equals("off") && !args[1].equals("boundaries")) {
            throw new IllegalArgumentException("REPORTS_DIRECTORY [boundaries|off]");
        }
        final boolean boundaries = args.length == 1 || args[1].equals("boundaries");
        final Path run = Path.of(args[0]).toAbsolutePath().resolve("run-" + UUID.randomUUID());
        Files.createDirectories(run);
        final long origin = System.nanoTime(), epochMicros = System.currentTimeMillis() * 1_000;
        final Source json = new Source(false, origin, epochMicros), sbe = new Source(true, origin, epochMicros);
        final Audit audit = new Audit(origin, boundaries, json, sbe);
        Throwable failure = null;
        try {
            final Path bundle = Path.of(CollectorIngestProfile.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.isRegularFile(bundle)) { throw new IllegalStateException("launch the retained bundled profile jar"); }
            final String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(bundle)));
            final Path artifact = run.resolve("artifacts").resolve(sha + ".jar");
            Files.createDirectories(artifact.getParent()); Files.copy(bundle, artifact);
            manifest(run, sha, audit, json, sbe);
            run(run, artifact, sha, origin, json, sbe, audit);
        } catch (final Throwable problem) { failure = problem; }
        finally {
            samples(run, json, sbe);
            report(run, audit, json, sbe, failure);
        }
        System.out.println("Collector ingestion profile retained at " + run);
        if (failure != null) { throw new IllegalStateException("collector ingestion profile failed; retained " + run, failure); }
    }

    private static void run(final Path directory, final Path artifact, final String sha, final long origin,
        final Source json, final Source sbe, final Audit audit) throws Exception {
        final CountDownLatch start = new CountDownLatch(1);
        try (Peer jsonPeer = new Peer(json, start); Peer sbePeer = new Peer(sbe, start);
             ClusterFixture fixture = new ClusterFixture(directory.resolve("state"), new SequencerService(), true,
                 false, TimeUnit.SECONDS.toNanos(120));
             AeronArchive archive = fixture.connectArchive(8100)) {
            fixture.expectClosedConsumerControlResponse(archive);
            fixture.connect();
            final Wire wire = new Wire();
            final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            final byte[] reply = fixture.exchange(command, wire.encodeInitialize(command, 0, 42));
            require(wire.validResult(new UnsafeBuffer(reply), 0, reply.length)
                && wire.result.status() == AdmissionStatus.INITIALIZED, "log initialization rejected");
            try (CapturedIngress jsonInput = input(fixture, archive, json); CapturedIngress sbeInput = input(fixture, archive, sbe);
                 Observer observer = audit.boundaries ? new Observer(archive, fixture, audit) : null) {
                audit.observer = observer;
                define(fixture, jsonInput, json, origin, artifact, sha);
                define(fixture, sbeInput, sbe, origin, artifact, sha);
                fixture.takeSnapshot();
                try (CatalogSubscription output = new CatalogSubscription(archive, 42, SequencerService.OUTPUT_STREAM, 0, IPC, 8350)) {
                    final EventHandler handler = audit::onEvent;
                    final WebSocketCollector jsonCollector = new WebSocketCollector(jsonInput, 1, 1, origin,
                        jsonPeer.uri(), SUBSCRIBE, null);
                    WebSocketCollector sbeCollector = null;
                    try {
                        sbeCollector = new WebSocketCollector(sbeInput, 1, 1, origin, sbePeer.uri(), SUBSCRIBE,
                            null, Map.of(), true);
                        final BackoffIdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 100_000);
                        final long startup = System.nanoTime() + DRAIN;
                        while (audit.acknowledgements != 2 || !jsonInput.idle() || !sbeInput.idle()
                            || observer != null && !observer.started()) {
                            fixture.assertHealthy(); jsonPeer.check(); sbePeer.check(); fixture.pollAdmin();
                            audit.ownerLoop();
                            final int collected = jsonCollector.doWork() + sbeCollector.doWork();
                            idle.idle(collected + (observer == null ? 0 : observer.poll()) + output.poll(handler, 256));
                            require(System.nanoTime() - startup < 0, "startup definition/acknowledgement fence timeout");
                        }
                        final long scheduled = System.nanoTime() + 100_000_000L;
                        json.schedule = sbe.schedule = scheduled;
                        audit.measurementStart = scheduled - origin;
                        start.countDown();
                        final long deadline = scheduled + (long)(WARMUP + BASELINE) * SECOND / RATE + DRAIN;
                        while (json.observed != json.raw.length || sbe.observed != sbe.raw.length
                            || json.sent != json.raw.length || sbe.sent != sbe.raw.length || !jsonInput.idle() || !sbeInput.idle()
                            || observer != null && !observer.complete()) {
                            fixture.assertHealthy(); jsonPeer.check(); sbePeer.check(); fixture.pollAdmin();
                            audit.polls++;
                            if (!jsonInput.idle()) { audit.jsonBusyPolls++; }
                            if (!sbeInput.idle()) { audit.sbeBusyPolls++; }
                            audit.ownerLoop();
                            final int collected = jsonCollector.doWork() + sbeCollector.doWork();
                            idle.idle(collected + (observer == null ? 0 : observer.poll()) + output.poll(handler, 256));
                            require(System.nanoTime() - deadline < 0, "fixed workload final drain timeout");
                        }
                        audit.measurementEnd = System.nanoTime() - origin;
                        audit.receiptDrainComplete = true;
                        fixture.takeSnapshot();
                        audit.snapshotFenceComplete = true;
                    } finally {
                        try { if (sbeCollector != null) { sbeCollector.onClose(); } }
                        finally { jsonCollector.onClose(); }
                    }
                }
            }
            fixture.abort();
            audit.finalHead = RecordingCatalog.rebuild(archive, 42, SequencerService.OUTPUT_STREAM, IPC, 8351).headSequence();
            countCaptured(archive, json, 8410); countCaptured(archive, sbe, 8411);
            require(audit.finalHead == json.raw.length + sbe.raw.length + 4 && audit.sequence == audit.finalHead,
                "canonical output does not account for every definition/acknowledgement/planned frame");
            for (final Source source : new Source[]{json, sbe}) {
                require(source.sent == source.raw.length && source.observed == source.raw.length
                    && source.captured == source.raw.length + 2, "send/submission/admission accounting differs");
            }
            fixture.assertHealthy();
        }
    }

    private static CapturedIngress input(final ClusterFixture fixture, final AeronArchive archive, final Source source) {
        return new CapturedIngress(archive, fixture.producerContext(), 42, source.producer(), source.id(),
            8210 + (source.binary ? 1 : 0), 8310 + (source.binary ? 1 : 0), null);
    }
    private static void define(final ClusterFixture fixture, final CapturedIngress input, final Source source,
        final long origin, final Path artifact, final String sha) {
        final String profile = "{\"profile\":\"spine.capture.v1\",\"venue\":\"scripted-binance-spot\",\"channel\":\"trade\","
            + "\"parser\":{\"artifact\":" + quote(artifact.toUri().toString()) + ",\"sha256\":" + quote(sha)
            + ",\"entrypoint\":" + quote(CollectorIngestProfile.class.getName()) + ",\"parameters\":{\"fixture\":true,\"format\":"
            + quote(source.binary ? "SBE" : "JSON") + ",\"batchTrades\":" + (source.binary ? 4 : 1) + "}},"
            + "\"raw\":{\"boundary\":\"complete WebSocket application message\",\"representation\":"
            + quote(source.binary ? "uint8 tag:0=UTF-8,1=binary" : "UTF-8")
            + ",\"transformation\":\"JDK WebSocket reassembly; text strictly reencoded UTF-8; RFC controls excluded\"},"
            + "\"native\":{\"namespace\":\"scripted fixture Spot BTCUSDT trade id\",\"representation\":\"absent; exact raw message retained\"},"
            + "\"clock\":{\"domain\":\"shared process monotonic elapsed nanoseconds\",\"origin\":" + quote(Long.toString(origin))
            + ",\"resolutionNanos\":1,\"receive\":\"first application callback\",\"complete\":\"last application callback assembled\","
            + "\"ready\":\"before capture submission\"}}";
        final String[] labels = {source.binary ? "local-scripted-sbe-peer" : "local-scripted-json-peer", "BTCUSDT",
            "profile-session-1", WebSocketCollector.class.getName(), profile, "shared process elapsed nanoseconds; no epoch subtraction"};
        final byte[][] bytes = new byte[labels.length][];
        for (int i = 0; i < labels.length; i++) { bytes[i] = labels[i].getBytes(StandardCharsets.UTF_8); }
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final int length = new CaptureCodec().encodeDefinition(payload, 0, -1, bytes);
        source.definition = new byte[length]; payload.getBytes(0, source.definition);
        source.definitionReady = System.nanoTime() - origin;
        require(input.submit(1, 1, -1, -1, source.definitionReady, payload, 0, length), "definition submit refused");
        fixture.await(input::idle, input::poll, "profile source definition admission");
    }

    private static final class Source {
        final boolean binary;
        final long origin;
        final byte[][] raw;
        final byte[] acknowledgement;
        final Boundaries boundaries;
        byte[] definition;
        long definitionReady;
        int followerCommands, canonicalCommands;
        final int[] firstTrade, tradeCount;
        final long[] offset, attempted, completedSend, receive, complete, ready, observedAt;
        volatile long schedule;
        volatile int sent;
        int observed;
        long captured;
        boolean definitionSeen, acknowledgementSeen;

        Source(final boolean binary, final long origin, final long epochMicros) {
            this.binary = binary; this.origin = origin;
            acknowledgement = binary ? taggedAck() : ACK;
            final int frames = binary ? TRADES / 4 : TRADES;
            boundaries = new Boundaries(frames + 2);
            raw = new byte[frames][]; firstTrade = new int[frames]; tradeCount = new int[frames];
            offset = new long[frames]; attempted = empty(frames); completedSend = empty(frames);
            receive = empty(frames); complete = empty(frames); ready = empty(frames); observedAt = empty(frames);
            int trade = 0;
            for (int i = 0; i < frames; i++) {
                firstTrade[i] = trade;
                tradeCount[i] = binary ? 4 : 1;
                offset[i] = (long)Math.min(trade, WARMUP + BASELINE) * SECOND / RATE;
                if (binary) {
                    final ByteBuffer encoded = ByteBuffer.allocate(1 + 8 + 18 + 6 + tradeCount[i] * 25 + 8).order(ByteOrder.LITTLE_ENDIAN);
                    encoded.put((byte)1).putShort((short)18).putShort((short)10000).putShort((short)1).putShort((short)0)
                        .putLong(epochMicros).putLong(epochMicros).put((byte)-2).put((byte)-8)
                        .putShort((short)25).putInt(tradeCount[i]);
                    for (int child = 0; child < tradeCount[i]; child++) {
                        encoded.putLong(100_000 + trade + child).putLong(10_000_000).putLong(100_000).put((byte)1);
                    }
                    encoded.put((byte)7).put("BTCUSDT".getBytes(StandardCharsets.US_ASCII));
                    raw[i] = encoded.array();
                } else {
                    raw[i] = ("{\"e\":\"trade\",\"E\":" + epochMicros + ",\"s\":\"BTCUSDT\",\"t\":" + (100_000 + trade)
                        + ",\"p\":\"100000.00000000\",\"q\":\"0.00100000\",\"T\":" + epochMicros + ",\"m\":true,\"M\":true}")
                        .getBytes(StandardCharsets.UTF_8);
                }
                trade += tradeCount[i];
            }
            require(trade == TRADES, "planned trade accounting differs");
        }
        int id() { return binary ? 8 : 7; }
        int producer() { return binary ? 11 : 10; }
        String phase(final int index) { return firstTrade[index] < WARMUP ? "warmup" : firstTrade[index] < WARMUP + BASELINE ? "baseline" : "burst"; }
    }

    private static final class Audit {
        final long origin;
        final Source[] sources;
        final CaptureCodec codec = new CaptureCodec();
        final boolean boundaries;
        Observer observer;
        long previousOwnerLoop = -1, maxOwnerLoopGapNanos;
        long sequence, measurementStart = -1, measurementEnd = -1, finalHead = -1, polls, jsonBusyPolls, sbeBusyPolls;
        int acknowledgements;
        boolean receiptDrainComplete, snapshotFenceComplete;
        Audit(final long origin, final boolean boundaries, final Source json, final Source sbe) {
            this.origin = origin; this.boundaries = boundaries; sources = new Source[]{json, sbe};
        }
        void ownerLoop() {
            final long now = System.nanoTime() - origin;
            if (previousOwnerLoop >= 0) { maxOwnerLoopGapNanos = Math.max(maxOwnerLoopGapNanos, now - previousOwnerLoop); }
            previousOwnerLoop = now;
        }
        void onEvent(final DirectBuffer buffer, final int offset, final int length, final SequencedEventDecoder event) {
            final long observed = System.nanoTime() - origin;
            require(event.logId() == 42 && event.sequence() == sequence + 1 && event.sourceId() >= 7 && event.sourceId() <= 8
                && event.sourceConfigVersion() == 1 && event.clockId() == 1, "canonical sequence/source binding differs");
            final Source source = sources[(int)event.sourceId() - 7];
            final int body = offset + Wire.EVENT_OVERHEAD, size = length - Wire.EVENT_OVERHEAD;
            if (codec.validDefinition(buffer, body, size)) {
                require(!source.definitionSeen && !source.acknowledgementSeen && source.observed == 0
                    && event.receiveTimeNanos() == -1 && event.completeTimeNanos() == -1 && event.readyTimeNanos() == source.definitionReady,
                    "duplicate/out-of-order source definition or invalid definition times");
                exact(buffer, body, size, source.definition);
                source.definitionSeen = true;
            } else {
                require(source.definitionSeen, "observation arrived before its source definition");
                require(codec.validObservation(buffer, body, size) && codec.observation.subeventIndex() == 0
                    && codec.observation.nativeNumericId() == CaptureCodec.UNKNOWN_NATIVE_ID && codec.nativeLength == 0,
                    "unexpected canonical capture payload");
                final long frame = codec.observation.rawFrameSequence();
                if (frame == 1) {
                    require(!source.acknowledgementSeen && source.observed == 0, "duplicate/out-of-order source acknowledgement");
                    exact(buffer, codec.rawOffset, codec.rawLength, source.acknowledgement);
                    source.acknowledgementSeen = true;
                    acknowledgements++;
                } else {
                    require(source.acknowledgementSeen, "data arrived before source acknowledgement");
                    final int index = source.observed;
                    require(index < source.raw.length && frame == index + 2L, "raw frame sequence differs from planned workload");
                    exact(buffer, codec.rawOffset, codec.rawLength, source.raw[index]);
                    require(event.receiveTimeNanos() >= 0 && event.receiveTimeNanos() <= event.completeTimeNanos()
                        && event.completeTimeNanos() <= event.readyTimeNanos() && event.readyTimeNanos() <= observed,
                        "shared-origin timestamp ordering differs");
                    source.receive[index] = event.receiveTimeNanos(); source.complete[index] = event.completeTimeNanos();
                    source.ready[index] = event.readyTimeNanos(); source.observedAt[index] = observed;
                    source.observed++;
                }
            }
            require(event.readyTimeNanos() <= observed, "recorded follower timestamp precedes ready");
            final int command = source.followerCommands++;
            source.boundaries.mark(command, 4, event.receiveTimeNanos(), event.completeTimeNanos(), event.readyTimeNanos(),
                event.sequence(), event.sequencedTimeNanos());
            source.boundaries.follower[command] = observed;
            sequence = event.sequence();
        }
    }

    /** Bounded diagnostics only; all mutable observer state belongs to the existing profile owner. */
    private static final class Boundaries {
        final int[] seen;
        final long[] receive, complete, ready, sequence, sequencedTime, follower;
        final long[] captureObserved, capturePosition, captureLower, captureUpper;
        final long[] canonicalObserved, canonicalPosition, outputLower, outputUpper;
        Boundaries(final int commands) {
            seen = new int[commands]; receive = empty(commands); complete = empty(commands); ready = empty(commands);
            sequence = empty(commands); sequencedTime = empty(commands); follower = empty(commands);
            captureObserved = empty(commands); capturePosition = empty(commands); captureLower = empty(commands); captureUpper = empty(commands);
            canonicalObserved = empty(commands); canonicalPosition = empty(commands); outputLower = empty(commands); outputUpper = empty(commands);
        }
        void mark(final int command, final int bit, final long received, final long completed, final long submitted,
            final long canonicalSequence, final long loggedTime) {
            require(command >= 0 && command < seen.length && (seen[command] & bit) == 0, "duplicate/out-of-bounds observer command");
            if (seen[command] != 0) {
                require(receive[command] == received && complete[command] == completed && ready[command] == submitted,
                    "capture/publication/follower observation times differ");
            } else { receive[command] = received; complete[command] = completed; ready[command] = submitted; }
            if (bit != 1) {
                if ((seen[command] & 6) != 0) {
                    require(sequence[command] == canonicalSequence && sequencedTime[command] == loggedTime,
                        "canonical publication and recorded follower sequence/logged time differ");
                } else { sequence[command] = canonicalSequence; sequencedTime[command] = loggedTime; }
            }
            seen[command] |= bit;
        }
    }

    private static final class Observer implements AutoCloseable {
        final Audit audit;
        final Stream[] streams = new Stream[3];
        long polls, totalPollNanos, maxPollNanos;
        Observer(final AeronArchive archive, final ClusterFixture fixture, final Audit audit) {
            this.audit = audit;
            try {
                streams[0] = new Stream(archive, fixture, audit, audit.sources[0], 8210, "spine-capture-10");
                streams[1] = new Stream(archive, fixture, audit, audit.sources[1], 8211, "spine-capture-11");
                streams[2] = new Stream(archive, fixture, audit, null, SequencerService.OUTPUT_STREAM, "spine-output");
            } catch (final RuntimeException | Error failure) { close(); throw failure; }
        }
        int poll() {
            final long start = System.nanoTime(); int work = 0;
            for (final Stream stream : streams) { work += stream.poll(); }
            final long elapsed = System.nanoTime() - start;
            polls++; totalPollNanos += elapsed; maxPollNanos = Math.max(maxPollNanos, elapsed);
            return work;
        }
        boolean started() {
            for (final Source source : audit.sources) {
                if (source.boundaries.seen[1] != 7 || source.boundaries.captureUpper[1] < 0 || source.boundaries.outputUpper[1] < 0) { return false; }
            }
            return true;
        }
        boolean complete() {
            for (final Stream stream : streams) { if (stream.observed != stream.commands.length || stream.fenced != stream.observed) { return false; } }
            for (final Source source : audit.sources) {
                for (final int seen : source.boundaries.seen) { if (seen != 7) { return false; } }
            }
            for (final Stream stream : streams) { require(stream.assembler.bufferBuilder().limit() == 0, "partial observer message at final drain"); }
            return true;
        }
        @Override public void close() { for (final Stream stream : streams) { if (stream != null) { stream.close(); } } }
    }

    private static final class Stream implements AutoCloseable {
        final Audit audit;
        final Source source;
        final Subscription subscription;
        final Image image;
        final ImageFragmentAssembler assembler;
        final Wire wire = new Wire();
        final CaptureCodec codec = new CaptureCodec();
        final FragmentHandler fragments = this::onFragment;
        final CountersReader counters;
        final long archiveId, recordingId, startPosition, registration;
        final int streamId, sessionId, counter;
        final Source[] commands;
        final int[] commandIndices;
        int observed, fenced, assemblingTermId;
        long lastPosition, lastReadStart = -1, lastRecordedPosition = -1;
        Throwable failure;
        Stream(final AeronArchive archive, final ClusterFixture fixture, final Audit audit, final Source source,
            final int streamId, final String alias) {
            this.audit = audit; this.source = source; this.streamId = streamId;
            archiveId = archive.archiveId(); counters = archive.context().aeron().countersReader();
            commands = new Source[source == null ? audit.sources[0].raw.length + audit.sources[1].raw.length + 4 : source.raw.length + 2];
            commandIndices = new int[commands.length];
            subscription = archive.context().aeron().addSubscription(IPC, streamId);
            try {
                final long[] descriptor = {-1, -1, -1};
                require(archive.listRecordings(0, 100, (control, correlation, id, started, stopped, start, stop,
                    termId, segment, termLength, mtu, session, stream, stripped, original, identity) -> {
                    if (stream == streamId && alias.equals(ChannelUri.parse(original).get("alias"))) {
                        require(descriptor[0] == -1 && stop == AeronArchive.NULL_POSITION, "observer recording generation differs");
                        descriptor[0] = id; descriptor[1] = start; descriptor[2] = session;
                    }
                }) < 100 && descriptor[0] >= 0, "observer recording absent/ambiguous catalog");
                recordingId = descriptor[0]; startPosition = descriptor[1]; sessionId = (int)descriptor[2];
                final long deadline = System.nanoTime() + DRAIN;
                Image joined;
                while ((joined = subscription.imageBySessionId(sessionId)) == null) {
                    fixture.assertHealthy(); fixture.pollAdmin();
                    require(System.nanoTime() - deadline < 0, "observer image join timeout"); LockSupport.parkNanos(100_000);
                }
                image = joined;
                require(subscription.imageCount() == 1 && image.joinPosition() == startPosition && image.position() == startPosition,
                    "observer missed initial recording bytes or has multiple images");
                counter = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
                require(counter != CountersReader.NULL_COUNTER_ID, "observer recording counter absent");
                long registered;
                while ((registered = counters.getCounterRegistrationId(counter)) == CountersReader.DEFAULT_REGISTRATION_ID) {
                    fixture.assertHealthy(); fixture.pollAdmin();
                    require(System.nanoTime() - deadline < 0, "observer registration timeout"); LockSupport.parkNanos(100_000);
                }
                registration = registered; validate();
                require(RecordingPos.findCounterIdBySession(counters, sessionId, archiveId) == counter,
                    "observer counter session/archive binding changed"); validate();
                lastPosition = startPosition;
                assembler = new ImageFragmentAssembler(this::onMessage,
                    source == null ? Wire.MAX_EVENT_BYTES : Wire.MAX_PROPOSAL_BYTES, false);
            } catch (final RuntimeException | Error problem) { subscription.close(); throw problem; }
        }
        void validate() {
            require(counters.getCounterRegistrationId(counter) == registration
                && RecordingPos.isActive(counters, counter, recordingId), "observer recording stopped/counter reused");
        }
        int poll() {
            require(!image.isClosed() && subscription.imageCount() == 1 && image.sessionId() == sessionId,
                "observer pinned image lost/replaced");
            int work = image.poll(fragments, 256);
            if (failure != null) { throw new IllegalStateException("observer callback failed", failure); }
            final long start = System.nanoTime() - audit.origin;
            validate(); final long position = counters.getCounterValue(counter); validate();
            final long end = System.nanoTime() - audit.origin;
            require(position >= lastRecordedPosition && (position & 31) == 0, "observer RecordingPos regressed/unaligned");
            for (int i = fenced; i < observed; i++) {
                final Boundaries b = commands[i].boundaries; final int command = commandIndices[i];
                final long target = source == null ? b.canonicalPosition[command] : b.capturePosition[command];
                final long[] lower = source == null ? b.outputLower : b.captureLower;
                final long[] upper = source == null ? b.outputUpper : b.captureUpper;
                if (position >= target) {
                    require(b.ready[command] <= end && (lower[command] < 0 || lower[command] <= end), "invalid observer fence bracket");
                    upper[command] = end; fenced++; work++;
                } else { lower[command] = start; }
            }
            lastRecordedPosition = position; lastReadStart = start;
            return work;
        }
        void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (failure != null) { return; }
            try {
                final int maximum = source == null ? Wire.MAX_EVENT_BYTES : Wire.MAX_PROPOSAL_BYTES;
                final boolean begin = (header.flags() & FrameDescriptor.BEGIN_FRAG_FLAG) != 0;
                require(begin ? assembler.bufferBuilder().limit() == 0
                    : assembler.bufferBuilder().limit() > 0 && header.termId() == assemblingTermId
                    && header.termOffset() == assembler.bufferBuilder().nextTermOffset(),
                    "observer nested/discontinuous/orphaned fragment");
                require(length <= maximum && (begin || assembler.bufferBuilder().limit() + length <= maximum),
                    "observer fragment exceeds declared message bound");
                if (begin) { assemblingTermId = header.termId(); }
                assembler.onFragment(buffer, offset, length, header);
            } catch (final Throwable problem) { failure = problem; }
        }
        void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            final boolean canonical = source == null;
            require(canonical ? wire.validEvent(buffer, offset, length) : wire.validProposal(buffer, offset, length),
                "invalid complete observer envelope");
            final Source current = canonical ? audit.sources[sourceIndex(wire.event.sourceId())] : source;
            final int command = canonical ? current.canonicalCommands : observed;
            final long received = canonical ? wire.event.receiveTimeNanos() : wire.proposal.receiveTimeNanos();
            final long completed = canonical ? wire.event.completeTimeNanos() : wire.proposal.completeTimeNanos();
            final long ready = canonical ? wire.event.readyTimeNanos() : wire.proposal.readyTimeNanos();
            require(canonical ? wire.event.logId() == 42 && wire.event.sequence() == observed + 1L
                && wire.event.sourceConfigVersion() == 1 && wire.event.clockId() == 1
                : wire.proposal.logId() == 42 && wire.proposal.producerId() == current.producer()
                && wire.proposal.sourceId() == current.id() && wire.proposal.producerSequence() == command + 1L
                && wire.proposal.sourceConfigVersion() == 1 && wire.proposal.clockId() == 1, "observer identity/order differs");
            require(command < current.boundaries.seen.length && observed < commands.length, "observer command bound exceeded");
            final int body = offset + (canonical ? Wire.EVENT_OVERHEAD : Wire.PROPOSAL_OVERHEAD), size = length - (body - offset);
            if (command == 0) {
                require(codec.validDefinition(buffer, body, size) && received == -1 && completed == -1
                    && ready == current.definitionReady, "observer source definition/time differs");
                exact(buffer, body, size, current.definition);
            } else {
                require(codec.validObservation(buffer, body, size) && codec.observation.rawFrameSequence() == command
                    && codec.observation.subeventIndex() == 0 && codec.nativeLength == 0
                    && codec.observation.nativeNumericId() == CaptureCodec.UNKNOWN_NATIVE_ID,
                    "observer raw-frame lineage differs");
                exact(buffer, codec.rawOffset, codec.rawLength, command == 1 ? current.acknowledgement : current.raw[command - 2]);
            }
            final long position = header.position();
            require(position > lastPosition && (position & 31) == 0 && header.sessionId() == sessionId,
                "observer complete message position/session differs");
            final Boundaries b = current.boundaries;
            b.mark(command, canonical ? 2 : 1, received, completed, ready,
                canonical ? wire.event.sequence() : -1, canonical ? wire.event.sequencedTimeNanos() : -1);
            if (canonical) { b.canonicalPosition[command] = position; current.canonicalCommands++; }
            else { b.capturePosition[command] = position; }
            final long[] lower = canonical ? b.outputLower : b.captureLower;
            if (lastReadStart >= 0 && lastRecordedPosition < position) { lower[command] = lastReadStart; }
            commands[observed] = current; commandIndices[observed++] = command; lastPosition = position;
            final long now = System.nanoTime() - audit.origin;
            require(ready <= now, "observer completed-message timestamp precedes ready");
            if (canonical) { b.canonicalObserved[command] = now; } else { b.captureObserved[command] = now; }
        }
        @Override public void close() { subscription.close(); }
    }
    private static int sourceIndex(final long source) {
        require(source == 7 || source == 8, "observer canonical source absent from fixed fresh bindings"); return (int)source - 7;
    }

    private static void countCaptured(final AeronArchive archive, final Source source, final int replayStream) {
        final long[] recording = {-1, -1, -1};
        archive.listRecordings(0, 100, (control, correlation, id, startTime, stopTime, start, stop,
            term, segment, termLength, mtu, session, stream, stripped, original, identity) -> {
            if (stream == 8210 + (source.binary ? 1 : 0)
                && ("spine-capture-" + source.producer()).equals(ChannelUri.parse(original).get("alias"))) {
                require(recording[0] == -1, "multiple capture generations in fresh profile");
                recording[0] = id; recording[1] = start; recording[2] = stop;
            }
        });
        require(recording[0] >= 0, "capture recording absent");
        final long deadline = System.nanoTime() + DRAIN;
        while (recording[2] == AeronArchive.NULL_POSITION) {
            recording[2] = archive.getStopPosition(recording[0]);
            require(System.nanoTime() - deadline < 0, "complete capture close fence timeout");
            LockSupport.parkNanos(100_000);
        }
        final Wire wire = new Wire();
        final CaptureCodec codec = new CaptureCodec();
        final Throwable[] failure = {null};
        final ImageFragmentAssembler assembler = new ImageFragmentAssembler((buffer, offset, length, header) -> {
            if (failure[0] != null) { return; }
            try {
                require(wire.validProposal(buffer, offset, length) && wire.proposal.logId() == 42
                    && wire.proposal.producerId() == source.producer() && wire.proposal.sourceId() == source.id()
                    && wire.proposal.sourceConfigVersion() == 1 && wire.proposal.clockId() == 1
                    && wire.proposal.producerSequence() == source.captured + 1, "captured proposal sequence/binding differs");
                final int body = offset + Wire.PROPOSAL_OVERHEAD, size = length - Wire.PROPOSAL_OVERHEAD;
                require(source.captured < source.boundaries.seen.length, "retained capture command bound exceeded");
                if (source.captured == 0) {
                    require(codec.validDefinition(buffer, body, size), "capture lacks initial definition");
                    exact(buffer, body, size, source.definition);
                }
                else {
                    require(codec.validObservation(buffer, body, size) && codec.observation.rawFrameSequence() == source.captured
                        && codec.observation.subeventIndex() == 0 && codec.nativeLength == 0
                        && codec.observation.nativeNumericId() == CaptureCodec.UNKNOWN_NATIVE_ID, "captured observation lineage differs");
                    final int index = (int)source.captured - 2;
                    exact(buffer, codec.rawOffset, codec.rawLength, index < 0 ? source.acknowledgement : source.raw[index]);
                }
                final int command = (int)source.captured;
                require(wire.proposal.receiveTimeNanos() == source.boundaries.receive[command]
                    && wire.proposal.completeTimeNanos() == source.boundaries.complete[command]
                    && wire.proposal.readyTimeNanos() == source.boundaries.ready[command],
                    "retained capture and live publication/recorded follower observation times differ");
                source.captured++;
            } catch (final Throwable problem) { failure[0] = problem; }
        }, Wire.MAX_PROPOSAL_BYTES, false);
        try (Subscription subscription = archive.context().aeron().addSubscription(IPC, replayStream)) {
            final long replay = archive.startReplay(recording[0], recording[1], recording[2] - recording[1], IPC, replayStream);
            try {
                Image image = null;
                while (image == null || image.position() < recording[2]) {
                    if (image == null) { image = subscription.imageBySessionId((int)replay); }
                    final int work = image == null ? 0 : image.poll(assembler, 256);
                    if (failure[0] != null) { throw new IllegalStateException("captured proposal audit failed", failure[0]); }
                    require(System.nanoTime() - deadline < 0 && (image == null || !image.isClosed() || image.position() >= recording[2]),
                        "captured proposal replay stopped before fence");
                    if (work == 0) { LockSupport.parkNanos(100_000); }
                }
                require(assembler.bufferBuilder().limit() == 0, "partial closed capture proposal");
            } finally {
                try { archive.stopReplay(replay); }
                catch (final ArchiveException error) { if (error.errorCode() != ArchiveException.UNKNOWN_REPLAY) { throw error; } }
            }
        }
    }

    private static final class Peer implements AutoCloseable {
        final ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
        final CountDownLatch stop = new CountDownLatch(1);
        final Thread thread;
        volatile Socket socket;
        volatile boolean closing;
        volatile Throwable failure;
        Peer(final Source source, final CountDownLatch start) throws Exception {
            server.setSoTimeout(10_000);
            thread = Thread.ofPlatform().name(source.binary ? "profile-sbe-peer" : "profile-json-peer").start(() -> {
                try (Socket connected = server.accept()) {
                    socket = connected; connected.setSoTimeout(10_000);
                    final InputStream input = connected.getInputStream(); final OutputStream output = connected.getOutputStream();
                    final String request = headers(input);
                    final String key = Arrays.stream(request.split("\r\n")).filter(line -> line.toLowerCase().startsWith("sec-websocket-key:"))
                        .map(line -> line.substring(line.indexOf(':') + 1).trim()).findFirst().orElseThrow();
                    final String accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1")
                        .digest((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)));
                    output.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                        + accept + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII)); output.flush();
                    require(octet(input) == 0x81, "unexpected profile subscription opcode");
                    final int flags = octet(input); require((flags & 0x80) != 0 && (flags & 0x7F) < 126, "invalid profile subscription frame");
                    final byte[] mask = input.readNBytes(4), message = input.readNBytes(flags & 0x7F);
                    require(mask.length == 4 && message.length == (flags & 0x7F), "truncated profile subscription");
                    for (int i = 0; i < message.length; i++) { message[i] ^= mask[i & 3]; }
                    require(Arrays.equals(message, SUBSCRIBE.getBytes(StandardCharsets.UTF_8)), "profile subscription differs");
                    send(output, 1, ACK, 0);
                    require(start.await(30, TimeUnit.SECONDS), "profile schedule barrier timeout");
                    for (int i = 0; i < source.raw.length && !closing; i++) {
                        final long intended = source.schedule + source.offset[i];
                        while (!closing && System.nanoTime() - intended < 0) {
                            LockSupport.parkNanos(Math.min(100_000, intended - System.nanoTime()));
                        }
                        if (closing) { break; }
                        source.attempted[i] = System.nanoTime() - source.origin;
                        send(output, source.binary ? 2 : 1, source.raw[i], source.binary ? 1 : 0);
                        source.completedSend[i] = System.nanoTime() - source.origin;
                        source.sent = i + 1;
                    }
                    stop.await(40, TimeUnit.SECONDS);
                } catch (final Throwable problem) { if (!closing) { failure = problem; } }
            });
        }
        URI uri() { return URI.create("ws://127.0.0.1:" + server.getLocalPort() + "/ws"); }
        void check() { if (failure != null) { throw new IllegalStateException("profile peer failed", failure); } }
        @Override public void close() throws Exception {
            closing = true; stop.countDown(); server.close(); if (socket != null) { socket.close(); }
            thread.join(10_000); require(!thread.isAlive(), "profile peer failed to terminate"); check();
        }
        private static String headers(final InputStream input) throws Exception {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(); int tail = 0;
            while (bytes.size() < 16_384) {
                final int value = octet(input); bytes.write(value); tail = tail << 8 | value;
                if (tail == 0x0D0A0D0A) { return bytes.toString(StandardCharsets.US_ASCII); }
            }
            throw new IllegalStateException("oversized profile handshake");
        }
        private static int octet(final InputStream input) throws Exception {
            final int value = input.read(); if (value < 0) { throw new EOFException("profile peer disconnected"); } return value;
        }
        private static void send(final OutputStream output, final int opcode, final byte[] raw, final int offset) throws Exception {
            final int length = raw.length - offset; output.write(0x80 | opcode);
            if (length < 126) { output.write(length); } else { output.write(126); output.write(length >>> 8); output.write(length & 0xFF); }
            output.write(raw, offset, length); output.flush();
        }
    }

    private static void manifest(final Path directory, final String sha, final Audit audit, final Source json, final Source sbe) throws Exception {
        final String value = "{\"workload\":\"fixed-local-collector-ingest-v1\",\"bundleSha256\":" + quote(sha)
            + ",\"observerMode\":" + quote(audit.boundaries ? "boundaries" : "off")
            + ",\"fixedSourceProducerBindings\":[{\"sourceId\":7,\"producerId\":10},{\"sourceId\":8,\"producerId\":11}]"
            + ",\"java\":" + quote(System.getProperty("java.vendor") + " " + System.getProperty("java.version"))
            + ",\"os\":" + quote(System.getProperty("os.name") + " " + System.getProperty("os.version") + " " + System.getProperty("os.arch"))
            + ",\"processors\":" + Runtime.getRuntime().availableProcessors() + ",\"maxHeapBytes\":" + Runtime.getRuntime().maxMemory()
            + ",\"aeron\":\"1.53.3\",\"agrona\":\"2.6.1\",\"sbe\":\"1.40.2\",\"driver\":\"SHARED\",\"termBytes\":1048576,\"ipcMtuBytes\":1024,"
            + "\"archiveFileSyncLevel\":0,\"clusterNodes\":1,\"clusterSessionTimeoutSeconds\":120,\"producerRetryNanos\":10000000,"
            + "\"ownerIdle\":\"Backoff(100,10,1000,100000)\",\"warmupSeconds\":2,\"warmupTradesPerSource\":300,"
            + "\"warmupMeaning\":\"diagnostic warmup budget, not steady JIT certification\",\"baselineSeconds\":4,\"baselineTradesPerSource\":600,"
            + "\"baselineJsonFramesPerSecond\":150,\"baselineSbeFramesPerSecond\":37.5,\"burstTradesPerSource\":128,\"finalDrainSeconds\":30,"
            + "\"jsonPlannedFrames\":" + json.raw.length + ",\"sbePlannedFrames\":" + sbe.raw.length
            + ",\"sbeBatchTrades\":4,\"originNanos\":" + json.origin
            + ",\"senderBoundary\":\"attempt before Socket write; completion after write+flush, not a kernel/network timestamp\","
            + "\"canonicalBoundary\":\"same-process callback on recorded-output Archive follower; includes recorder lag and consumer polling\","
            + "\"submissionEvidence\":\"unique complete retained capture proposals; no private submit-call hook\","
            + "\"observerBoundary\":\"complete pinned-image message after byte validation; pinned RecordingPos read bracket; upper bounds include owner polling lag\","
            + "\"observerCostsScope\":\"owner observer polls from startup definitions/ACK drain through final data drain; excludes cold pin/close and report I/O\","
            + "\"fenceLowerMeaning\":\"latest preceding below-target counter read START; -1 means unknown\","
            + "\"fenceUpperMeaning\":\"counter read END after active recording and registration validation\","
            + "\"canonicalProducerIdentity\":\"inferred from explicit fresh source binding and exact payload/order; absent from canonical envelope\","
            + "\"scope\":\"local scripted ingestion only; no exchange/network latency, encoding winner, whole-JDK allocation or host/power-loss claim\"}";
        Files.writeString(directory.resolve("manifest.json"), value + "\n");
    }
    private static final String OBSERVER_COLUMNS = "canonicalSequence,capturePublicationObservedNanos,captureEndPosition,captureFenceLowerNanos,captureFenceUpperNanos,canonicalPublicationObservedNanos,canonicalEndPosition,outputFenceLowerNanos,outputFenceUpperNanos";
    private static void samples(final Path directory, final Source... sources) throws Exception {
        try (BufferedWriter out = Files.newBufferedWriter(directory.resolve("frames.csv"));
             BufferedWriter commands = Files.newBufferedWriter(directory.resolve("commands.csv"))) {
            out.write("source,phase,frame,firstTradeId,trades,payloadBytes,intendedNanos,attemptedNanos,writeFlushCompletedNanos,receiveNanos,completeNanos,readyNanos,recordedConsumerObservedNanos,producerId,producerSequence," + OBSERVER_COLUMNS + "\n");
            commands.write("source,kind,producerId,producerSequence,frame,receiveNanos,completeNanos,readyNanos,recordedConsumerObservedNanos," + OBSERVER_COLUMNS + "\n");
            for (final Source source : sources) {
                for (int command = 0; command < source.boundaries.seen.length; command++) {
                    final Boundaries b = source.boundaries;
                    commands.write(source.id() + "," + (command == 0 ? "definition" : command == 1 ? "acknowledgement" : "data")
                        + "," + source.producer() + "," + (command + 1) + "," + command + "," + b.receive[command] + ","
                        + b.complete[command] + "," + b.ready[command] + "," + b.follower[command] + "," + boundarySamples(b, command) + "\n");
                }
                for (int i = 0; i < source.raw.length; i++) {
                    final long intended = source.schedule == 0 ? -1 : source.schedule - source.origin + source.offset[i];
                    out.write(source.id() + "," + source.phase(i) + "," + (i + 2) + "," + (100_000 + source.firstTrade[i]) + ","
                        + source.tradeCount[i] + "," + (source.raw[i].length - (source.binary ? 1 : 0)) + "," + intended + ","
                        + source.attempted[i] + "," + source.completedSend[i] + "," + source.receive[i] + "," + source.complete[i]
                        + "," + source.ready[i] + "," + source.observedAt[i] + "," + source.producer() + "," + (i + 3)
                        + "," + boundarySamples(source.boundaries, i + 2) + "\n");
                }
            }
        }
    }
    private static String boundarySamples(final Boundaries b, final int command) {
        if ((b.seen[command] & 3) == 0) { return b.sequence[command] + ",,,,,,,,"; }
        return b.sequence[command] + "," + b.captureObserved[command] + "," + b.capturePosition[command] + ","
            + b.captureLower[command] + "," + b.captureUpper[command] + "," + b.canonicalObserved[command] + ","
            + b.canonicalPosition[command] + "," + b.outputLower[command] + "," + b.outputUpper[command];
    }
    private static void report(final Path directory, final Audit audit, final Source json, final Source sbe, final Throwable failure) throws Exception {
        final StringBuilder value = new StringBuilder("{\"status\":").append(quote(failure == null ? "passed" : "failed"))
            .append(",\"failure\":").append(failure == null ? "null" : quote(failure.toString()))
            .append(",\"observerMode\":").append(quote(audit.boundaries ? "boundaries" : "off"))
            .append(",\"observer\":").append(observerReport(audit))
            .append(",\"measurementStartNanos\":").append(audit.measurementStart).append(",\"drainCompletedNanos\":").append(audit.measurementEnd)
            .append(",\"canonicalEventsObserved\":").append(audit.sequence).append(",\"completeCanonicalHead\":").append(audit.finalHead)
            .append(",\"acknowledgements\":").append(audit.acknowledgements).append(",\"ownerPolls\":").append(audit.polls)
            .append(",\"jsonPendingPolls\":").append(audit.jsonBusyPolls).append(",\"sbePendingPolls\":").append(audit.sbeBusyPolls)
            .append(",\"receiptDrainComplete\":").append(audit.receiptDrainComplete).append(",\"snapshotRecordingFenceComplete\":").append(audit.snapshotFenceComplete)
            .append(",\"sources\":[");
        boolean comma = false;
        for (final Source source : new Source[]{json, sbe}) {
            if (comma) { value.append(','); } comma = true;
            value.append("{\"sourceId\":").append(source.id()).append(",\"plannedTrades\":").append(TRADES)
                .append(",\"plannedFrames\":").append(source.raw.length).append(",\"attemptedFrames\":").append(count(source.attempted))
                .append(",\"completedWrites\":").append(source.sent).append(",\"completeSubmittedCommands\":").append(source.captured)
                .append(",\"admittedDataFramesFromCanonicalOutput\":").append(source.observed)
                .append(",\"capturePublicationCommands\":").append(count(source.boundaries.captureObserved))
                .append(",\"captureRecordingFences\":").append(count(source.boundaries.captureUpper))
                .append(",\"canonicalPublicationCommands\":").append(count(source.boundaries.canonicalObserved))
                .append(",\"outputRecordingFences\":").append(count(source.boundaries.outputUpper))
                .append(",\"recordedFollowerCommands\":").append(source.followerCommands)
                .append(",\"applicationPayloadBytes\":").append(source.raw[0].length - (source.binary ? 1 : 0))
                .append(",\"rawBytesPerDataFrame\":").append(source.raw[0].length).append(",\"phases\":[");
            for (int phase = 0; phase < 3; phase++) {
                if (phase != 0) { value.append(','); }
                final String name = phase == 0 ? "warmup" : phase == 1 ? "baseline" : "burst";
                int plannedFrames = 0;
                for (int i = 0; i < source.raw.length; i++) { if (source.phase(i).equals(name)) { plannedFrames++; } }
                value.append("{\"phase\":").append(quote(name)).append(",\"plannedFrames\":").append(plannedFrames)
                    .append(",\"plannedTrades\":").append(plannedFrames * (source.binary ? 4 : 1))
                    .append(",\"scheduleLatenessNanos\":").append(stats(source, name, 0))
                    .append(",\"plannedToReceiveNanos\":").append(stats(source, name, 1))
                    .append(",\"completeToReadyNanos\":").append(stats(source, name, 2))
                    .append(",\"readyToRecordedConsumerNanos\":").append(stats(source, name, 3))
                    .append(",\"readyToCaptureFenceUpperNanos\":").append(stats(source, name, 4))
                    .append(",\"captureFenceUpperToCanonicalPublicationNanos\":").append(stats(source, name, 5))
                    .append(",\"canonicalPublicationToOutputFenceUpperNanos\":").append(stats(source, name, 6))
                    .append(",\"outputFenceUpperToRecordedConsumerNanos\":").append(stats(source, name, 7))
                    .append(",\"captureFenceBracketNanos\":").append(stats(source, name, 8))
                    .append(",\"outputFenceBracketNanos\":").append(stats(source, name, 9)).append('}');
            }
            value.append("]}");
        }
        value.append("]}"); Files.writeString(directory.resolve("report.json"), value + "\n");
    }
    private static String observerReport(final Audit audit) {
        final Observer observer = audit.observer;
        final StringBuilder value = new StringBuilder("{\"polls\":").append(observer == null ? 0 : observer.polls)
            .append(",\"totalPollNanos\":").append(observer == null ? 0 : observer.totalPollNanos)
            .append(",\"maxPollNanos\":").append(observer == null ? 0 : observer.maxPollNanos)
            .append(",\"maxOwnerLoopGapNanos\":").append(audit.maxOwnerLoopGapNanos).append(",\"recordings\":[");
        if (observer != null) {
            for (int i = 0; i < observer.streams.length; i++) {
                final Stream stream = observer.streams[i]; if (i != 0) { value.append(','); }
                value.append("{\"source\":").append(stream.source == null ? 0 : stream.source.id())
                    .append(",\"streamId\":").append(stream.streamId).append(",\"recordingId\":").append(stream.recordingId)
                    .append(",\"sessionId\":").append(stream.sessionId).append(",\"startPosition\":").append(stream.startPosition)
                    .append(",\"counterId\":").append(stream.counter).append(",\"counterRegistrationId\":").append(stream.registration)
                    .append(",\"archiveId\":").append(stream.archiveId).append(",\"completeCommands\":").append(stream.observed)
                    .append(",\"recordingFences\":").append(stream.fenced).append('}');
            }
        }
        return value.append("]}").toString();
    }
    private static String stats(final Source source, final String phase, final int metric) {
        final long[] values = new long[source.raw.length]; int count = 0;
        for (int i = 0; i < source.raw.length; i++) {
            if (!source.phase(i).equals(phase)) { continue; }
            final Boundaries b = source.boundaries; final int command = i + 2;
            final long left = switch (metric) {
                case 0 -> source.attempted[i]; case 1 -> source.receive[i]; case 2 -> source.ready[i];
                case 3, 7 -> source.observedAt[i]; case 4, 8 -> b.captureUpper[command];
                case 5 -> b.canonicalObserved[command]; case 6, 9 -> b.outputUpper[command];
                default -> throw new IllegalArgumentException("metric");
            };
            final long right = switch (metric) {
                case 0, 1 -> source.schedule == 0 ? -1 : source.schedule - source.origin + source.offset[i];
                case 2 -> source.complete[i]; case 3, 4 -> source.ready[i]; case 5 -> b.captureUpper[command];
                case 6 -> b.canonicalObserved[command]; case 7 -> b.outputUpper[command];
                case 8 -> b.captureLower[command]; case 9 -> b.outputLower[command];
                default -> throw new IllegalArgumentException("metric");
            };
            if (left >= 0 && right >= 0) { values[count++] = left - right; }
        }
        if (count == 0) { return "{\"count\":0,\"min\":null,\"max\":null,\"p50\":null,\"p99\":null,\"p999\":null}"; }
        Arrays.sort(values, 0, count);
        return "{\"count\":" + count + ",\"min\":" + values[0] + ",\"max\":" + values[count - 1] + ",\"p50\":"
            + values[(count * 50 + 99) / 100 - 1] + ",\"p99\":" + values[(count * 99 + 99) / 100 - 1]
            + ",\"p999\":" + values[(count * 999 + 999) / 1000 - 1] + "}";
    }
    private static long[] empty(final int count) { final long[] values = new long[count]; Arrays.fill(values, -1); return values; }
    private static int count(final long[] values) { int count = 0; for (final long value : values) { if (value >= 0) { count++; } } return count; }
    private static byte[] taggedAck() { final byte[] bytes = new byte[ACK.length + 1]; System.arraycopy(ACK, 0, bytes, 1, ACK.length); return bytes; }
    private static void exact(final DirectBuffer buffer, final int offset, final int length, final byte[] expected) {
        require(length == expected.length, "raw frame length differs");
        for (int i = 0; i < length; i++) { require(buffer.getByte(offset + i) == expected[i], "raw frame bytes differ"); }
    }
    private static void require(final boolean condition, final String message) { if (!condition) { throw new IllegalStateException(message); } }
    private static String quote(final String text) {
        final StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            final char c = text.charAt(i);
            if (c == '"' || c == '\\') { result.append('\\').append(c); }
            else if (c < 32) { result.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int)c)); }
            else { result.append(c); }
        }
        return result.append('"').toString();
    }
}
