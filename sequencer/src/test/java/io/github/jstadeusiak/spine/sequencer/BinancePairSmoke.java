package io.github.jstadeusiak.spine.sequencer;

import io.aeron.archive.client.AeronArchive;
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.WebSocketHandshakeException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Test-only paired capture. The API key is read solely from the environment and never retained in provenance. */
public final class BinancePairSmoke {
    private static final long LOG = 42;
    private static final String IPC = "aeron:ipc?term-length=1m";
    private static final URI JSON_ENDPOINT = URI.create("wss://data-stream.binance.vision/ws?timeUnit=MICROSECOND");
    private static final URI SBE_ENDPOINT = URI.create("wss://stream-sbe.binance.com/ws");
    private static final String SUBSCRIPTION = "{\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@trade\"],\"id\":1}";
    private static final String SCHEMA_SHA = "6ea328467e144311b1f1efff38e9fe613829997f041dd02a3b7077885d10a1f7";
    private static final String SCHEMA_URL = "https://raw.githubusercontent.com/binance/binance-spot-api-docs/"
        + "bed5d358dbe98bfa4722b6c00c27be1e8f0ba40b/sbe/schemas/stream_1_0.xml";
    static final String LOCAL_PROVENANCE = "local scripted Binance-shape fixture; not venue data";

    static String localCredential() { return String.join("-", "spine", "local", "dummy"); }

    public static void main(final String[] args) throws Exception {
        if (args.length < 1 || args.length > 2) { throw new IllegalArgumentException("DATA_DIR [SECONDS=60]"); }
        final String key = System.getenv("BINANCE_SBE_API_KEY");
        if (key == null || key.isBlank()) { throw new IllegalArgumentException("BINANCE_SBE_API_KEY required"); }
        final boolean local = Boolean.getBoolean("spine.test.localSynthetic");
        if (local && !key.equals(localCredential())) { throw new IllegalArgumentException("local synthetic run requires its dummy credential"); }
        final URI jsonEndpoint = local ? localEndpoint("spine.test.jsonEndpoint") : JSON_ENDPOINT;
        final URI sbeEndpoint = local ? localEndpoint("spine.test.sbeEndpoint") : SBE_ENDPOINT;
        if (local && jsonEndpoint.equals(sbeEndpoint)) { throw new IllegalArgumentException("distinct local peers required"); }
        final long seconds = args.length == 2 ? Long.parseLong(args[1]) : 60;
        if (seconds < 1 || seconds > 86_400) { throw new IllegalArgumentException("seconds must be 1..86400"); }
        final Path data = Path.of(args[0]).toAbsolutePath();
        final Path bundle = Path.of(BinancePairSmoke.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (!Files.isRegularFile(bundle)) { throw new IllegalStateException("launch the retained paired smoke jar"); }
        final String sha = sha256(bundle);
        final Path artifact = data.resolve("artifacts").resolve(sha + ".jar");
        Files.createDirectories(artifact.getParent());
        if (!Files.exists(artifact)) { Files.copy(bundle, artifact); }
        if (!sha.equals(sha256(artifact))) { throw new IllegalStateException("retained executable digest differs"); }
        try (ClusterFixture fixture = new ClusterFixture(data, new SequencerService(), false, false, TimeUnit.SECONDS.toNanos(120));
             AeronArchive archive = fixture.connectArchive(8100)) {
            fixture.expectClosedConsumerControlResponse(archive);
            initialize(fixture);
            final long previousHead, origin, started, finished;
            final int clock, jsonConfiguration, sbeConfiguration;
            try {
                try (CapturedIngress json = new CapturedIngress(archive, fixture.producerContext(), LOG, 10, 7, 8210, 8410, null);
                     CapturedIngress sbe = new CapturedIngress(archive, fixture.producerContext(), LOG, 11, 8, 8211, 8411, null)) {
                    drain(fixture, json, sbe);
                    fixture.takeSnapshot(); // Existing recorder fence includes restored proposals before the prior head is read.
                    previousHead = catalog(archive, 8350).headSequence();
                    origin = System.nanoTime();
                    clock = next(Math.max(json.lastClockId(), sbe.lastClockId()));
                    jsonConfiguration = next(json.lastConfiguration());
                    sbeConfiguration = next(sbe.lastConfiguration());
                    define(fixture, json, jsonConfiguration, clock, origin, artifact, sha, false, local);
                    define(fixture, sbe, sbeConfiguration, clock, origin, artifact, sha, true, local);
                    final WebSocketCollector jsonSocket = new WebSocketCollector(json, jsonConfiguration, clock, origin,
                        jsonEndpoint, SUBSCRIPTION, null);
                    WebSocketCollector sbeSocket = null;
                    try {
                        try {
                            sbeSocket = new WebSocketCollector(sbe, sbeConfiguration, clock, origin, sbeEndpoint,
                                SUBSCRIPTION, null, Map.of("X-MBX-APIKEY", key), true);
                        } catch (final Exception failure) {
                            // Suppress network exception details rather than risk reflecting an authentication header.
                            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                                if (cause instanceof WebSocketHandshakeException handshake) {
                                    throw new IllegalStateException("SBE WebSocket open failed: HTTP " + handshake.getResponse().statusCode());
                                }
                            }
                            throw new IllegalStateException("SBE WebSocket open failed");
                        }
                        try (LocalCompletion completion = local ? new LocalCompletion(archive, previousHead, clock) : null) {
                            final BackoffIdleStrategy idle = new BackoffIdleStrategy(100, 10, 1_000, 100_000);
                            started = System.nanoTime() - origin;
                            if (local) {
                                Files.writeString(data.resolve("local-ready-" + Integer.toUnsignedString(clock) + ".json"),
                                    "{\"clockId\":" + Integer.toUnsignedLong(clock) + "}\n");
                            }
                            final long end = System.nanoTime() + seconds * 1_000_000_000L;
                            try {
                                while (System.nanoTime() - end < 0 || completion != null && !completion.done()) {
                                    fixture.assertHealthy();
                                    fixture.pollAdmin();
                                    idle.idle(jsonSocket.doWork() + sbeSocket.doWork() + (completion == null ? 0 : completion.poll()));
                                    if (completion != null && System.nanoTime() - end > TimeUnit.SECONDS.toNanos(30)) {
                                        throw new IllegalStateException("local corpus was not completely applied");
                                    }
                                }
                                jsonSocket.doWork(); sbeSocket.doWork();
                            } catch (final RuntimeException failure) { throw new IllegalStateException("paired WebSocket operation failed"); }
                            finished = System.nanoTime() - origin;
                        }
                    } finally {
                        try { if (sbeSocket != null) { sbeSocket.onClose(); } }
                        finally { jsonSocket.onClose(); }
                    }
                    drain(fixture, json, sbe);
                }
            } finally { fixture.abort(); } // Termination fences and closes the complete output recording before export.
            export(fixture, archive, data, clock, jsonConfiguration, sbeConfiguration, previousHead, started, finished,
                seconds, sha, jsonEndpoint, sbeEndpoint, local);
            fixture.assertHealthy();
        }
    }

    private static void initialize(final ClusterFixture fixture) {
        fixture.connect();
        final Wire wire = new Wire();
        final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        final byte[] reply = fixture.exchange(command, wire.encodeInitialize(command, 0, LOG));
        if (!wire.validResult(new UnsafeBuffer(reply), 0, reply.length) || wire.result.status() != AdmissionStatus.INITIALIZED) {
            throw new IllegalStateException("log initialization rejected");
        }
    }

    private static void drain(final ClusterFixture fixture, final CapturedIngress json, final CapturedIngress sbe) {
        fixture.await(() -> json.idle() && sbe.idle(), () -> { json.poll(); sbe.poll(); }, "both exact capture/admission fences");
    }

    private static void define(final ClusterFixture fixture, final CapturedIngress ingress, final int configuration,
        final int clock, final long origin, final Path artifact, final String sha, final boolean sbe, final boolean local) {
        final String profile = "{\"profile\":\"spine.capture.v1\",\"venue\":\"binance-spot\",\"channel\":\"trade\","
            + "\"parser\":{\"artifact\":" + quote(artifact.toUri().toString()) + ",\"sha256\":" + quote(sha)
            + ",\"entrypoint\":" + quote(WebSocketCollector.class.getName()) + ",\"parameters\":{\"format\":"
            + quote(sbe ? "SBE" : "JSON") + ",\"timeUnit\":\"MICROSECOND\",\"subscription\":" + SUBSCRIPTION
            + ",\"applicationHeartbeat\":null,\"rfcPingPong\":\"JDK WebSocket implementation\",\"java\":"
            + quote(System.getProperty("java.vendor") + " " + System.getProperty("java.version")) + ",\"harness\":"
            + quote(BinancePairSmoke.class.getName()) + ",\"exchangeTimestamps\":\"market data: Unix epoch microseconds; shutdown controls: milliseconds or microseconds\",\"taggedFraming\":" + sbe
            + ",\"maxApplicationPayloadBytes\":" + (sbe ? 65499 : 65500)
            + (sbe ? ",\"sbeSchemaId\":1,\"sbeSchemaVersion\":0,\"sbeSchemaSha256\":" + quote(SCHEMA_SHA)
                + ",\"sbeSchemaUrl\":" + quote(SCHEMA_URL) : "")
            + (local ? ",\"provenance\":" + quote(LOCAL_PROVENANCE) : "") + "}},"
            + "\"raw\":{\"boundary\":\"one complete WebSocket application message\",\"representation\":"
            + quote(sbe ? "tagged bytes: 0=UTF-8 text, 1=binary" : "UTF-8") + ",\"transformation\":"
            + quote(sbe ? "leading uint8 tag 0 for strictly reencoded JDK UTF-16 text, 1 for unchanged binary payload; RFC controls excluded"
                : "JDK UTF-16 callbacks reassembled and strictly reencoded UTF-8; RFC controls excluded") + "},"
            + "\"native\":{\"namespace\":\"binance Spot BTCUSDT trade id t\",\"representation\":\"absent; identity retained in raw message\"},"
            + "\"clock\":{\"domain\":\"shared process monotonic elapsed nanoseconds\",\"origin\":" + quote(Long.toString(origin))
            + ",\"resolutionNanos\":1,\"receive\":\"first application-message callback\",\"complete\":\"last application-message callback assembled\","
            + "\"ready\":\"immediately before capture submission\"}}";
        final String[] labels = {sbe ? "binance-sbe" : "binance-public", "BTCUSDT",
            (sbe ? "ws-sbe-" : "ws-json-") + Integer.toUnsignedString(configuration), WebSocketCollector.class.getName(),
            profile, "elapsed nanoseconds from retained shared origin; synchronization uncertainty unknown"};
        final byte[][] encoded = new byte[labels.length][];
        for (int i = 0; i < labels.length; i++) { encoded[i] = labels[i].getBytes(StandardCharsets.UTF_8); }
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final int length = new CaptureCodec().encodeDefinition(payload, 0, -1, encoded);
        if (!ingress.submit(configuration, clock, -1, -1, System.nanoTime() - origin, payload, 0, length)) {
            throw new IllegalStateException("source definition refused");
        }
        fixture.await(ingress::idle, ingress::poll, "immutable source definition admission");
    }

    private static RecordingCatalog catalog(final AeronArchive archive, final int replayStream) {
        return RecordingCatalog.rebuild(archive, LOG, SequencerService.OUTPUT_STREAM, IPC, replayStream);
    }

    private static void export(final ClusterFixture fixture, final AeronArchive archive, final Path data, final int clock,
        final int jsonConfiguration, final int sbeConfiguration, final long previousHead, final long started, final long finished,
        final long seconds, final String sha, final URI jsonEndpoint, final URI sbeEndpoint, final boolean local) throws IOException {
        final long head = catalog(archive, 8351).headSequence();
        final String suffix = Integer.toUnsignedString(clock);
        try (BufferedWriter output = Files.newBufferedWriter(data.resolve("canonical-" + suffix + ".jsonl"));
             BufferedWriter definitions = Files.newBufferedWriter(data.resolve("definitions-" + suffix + ".jsonl"));
             CatalogSubscription replay = new CatalogSubscription(archive, LOG, SequencerService.OUTPUT_STREAM, 0, IPC, 8352)) {
            final Audit audit = new Audit(output, definitions);
            final EventHandler handler = audit::onEvent;
            fixture.await(() -> replay.sequence() == head, () -> {
                if (replay.requiresRefresh()) { replay.refresh(); }
                replay.poll(handler, 256);
            }, "complete paired canonical output export");
            if (audit.sequence != head || audit.sources[0].configuration != Integer.toUnsignedLong(jsonConfiguration)
                || audit.sources[1].configuration != Integer.toUnsignedLong(sbeConfiguration)) {
                throw new IllegalStateException("final canonical cursor/configuration differs");
            }
            for (final Source source : audit.sources) {
                if (source.clock != Integer.toUnsignedLong(clock) || source.rawSequence == 0) {
                    throw new IllegalStateException("paired session has no captured application frames");
                }
            }
            final String report = "{\"mode\":" + quote(local ? "binance-spot-pair-local-synthetic" : "binance-spot-pair")
                + (local ? ",\"credentialKind\":\"local-dummy\"" : "") + ",\"jsonEndpoint\":" + quote(jsonEndpoint.toString())
                + ",\"sbeEndpoint\":" + quote(sbeEndpoint.toString()) + ",\"seconds\":" + seconds
                + ",\"clockId\":" + Integer.toUnsignedLong(clock) + ",\"canonicalHead\":" + head
                + ",\"previousCanonicalHead\":" + previousHead + ",\"measurementStartTimeNanos\":" + started
                + ",\"clusterSessionTimeoutNs\":" + TimeUnit.SECONDS.toNanos(120)
                + ",\"measurementEndTimeNanos\":" + finished + ",\"definitions\":" + audit.definitionCount
                + ",\"cumulativeFrames\":" + (audit.sources[0].frames + audit.sources[1].frames)
                + ",\"cumulativeRawBytes\":" + (audit.sources[0].bytes + audit.sources[1].bytes)
                + ",\"bundleSha256\":" + quote(sha) + ",\"timestampViolations\":0,\"sources\":["
                + audit.sources[0].report() + "," + audit.sources[1].report() + "]}";
            Files.writeString(data.resolve("run-" + suffix + ".json"), report + "\n");
            System.out.println(report);
        }
    }

    private static final class Source {
        final int id;
        long configuration, clock, rawSequence, frames, bytes;
        String session;
        Source(final int id) { this.id = id; }
        String report() {
            return "{\"sourceId\":" + id + ",\"configuration\":" + configuration + ",\"clockId\":" + clock
                + ",\"session\":" + quote(session) + ",\"currentSessionFrames\":" + rawSequence
                + ",\"cumulativeFrames\":" + frames + ",\"cumulativeRawBytes\":" + bytes + "}";
        }
    }

    /** Local acceptance completion uses actual independent canonical application, never elapsed-time guessing. */
    private static final class LocalCompletion implements AutoCloseable {
        final CatalogSubscription replay;
        final CaptureCodec codec = new CaptureCodec();
        final long[] frames = new long[2];
        final EventHandler handler;
        LocalCompletion(final AeronArchive archive, final long previousHead, final int clock) {
            replay = new CatalogSubscription(archive, LOG, SequencerService.OUTPUT_STREAM, previousHead, IPC, 8353);
            handler = (buffer, offset, length, event) -> {
                if (event.clockId() == Integer.toUnsignedLong(clock)
                    && codec.validObservation(buffer, offset + Wire.EVENT_OVERHEAD, length - Wire.EVENT_OVERHEAD)) {
                    final int source = (int)event.sourceId() - 7;
                    if (source < 0 || source > 1 || codec.observation.rawFrameSequence() != frames[source] + 1) {
                        throw new IllegalStateException("local completion source sequence differs");
                    }
                    frames[source]++;
                }
            };
        }
        int poll() { if (replay.requiresRefresh()) { replay.refresh(); } return replay.poll(handler, 256); }
        boolean done() {
            return frames[0] == BinancePairLocalSmoke.JSON_FRAMES && frames[1] == BinancePairLocalSmoke.SBE_FRAMES;
        }
        @Override public void close() { replay.close(); }
    }

    private static final class Audit {
        final BufferedWriter output, definitions;
        final CaptureCodec codec = new CaptureCodec();
        final Source[] sources = {new Source(7), new Source(8)};
        long sequence;
        int definitionCount;
        Audit(final BufferedWriter output, final BufferedWriter definitions) { this.output = output; this.definitions = definitions; }

        void onEvent(final DirectBuffer buffer, final int offset, final int length, final SequencedEventDecoder event) {
            if (event.logId() != LOG || event.sequence() != sequence + 1 || event.sourceId() < 7 || event.sourceId() > 8) {
                throw new IllegalStateException("noncontiguous or foreign canonical output");
            }
            final Source source = sources[(int)event.sourceId() - 7];
            final String metadata = "\"sequence\":" + event.sequence() + ",\"logId\":" + event.logId()
                + ",\"sourceId\":" + event.sourceId() + ",\"configuration\":" + event.sourceConfigVersion() + ",\"clockId\":" + event.clockId()
                + ",\"sequencedTimeNanos\":" + event.sequencedTimeNanos() + ",\"receiveTimeNanos\":" + event.receiveTimeNanos()
                + ",\"completeTimeNanos\":" + event.completeTimeNanos() + ",\"readyTimeNanos\":" + event.readyTimeNanos();
            final int body = offset + Wire.EVENT_OVERHEAD, size = length - Wire.EVENT_OVERHEAD;
            final String row;
            if (codec.validDefinition(buffer, body, size)) {
                if (event.sourceConfigVersion() <= source.configuration || event.clockId() <= source.clock
                    || event.receiveTimeNanos() != -1 || event.completeTimeNanos() != -1 || event.readyTimeNanos() < 0) {
                    throw new IllegalStateException("source bindings did not advance or definition times differ");
                }
                source.configuration = event.sourceConfigVersion(); source.clock = event.clockId(); source.rawSequence = 0;
                final String[] labels = new String[6];
                for (int i = 0; i < labels.length; i++) { labels[i] = utf8(buffer, codec.labelOffset(i), codec.labelLength(i)); }
                source.session = labels[2]; definitionCount++;
                row = "{\"kind\":\"definition\"," + metadata + ",\"provider\":" + quote(labels[0]) + ",\"product\":" + quote(labels[1])
                    + ",\"session\":" + quote(source.session) + ",\"collector\":" + quote(labels[3]) + ",\"profile\":" + quote(labels[4])
                    + ",\"clockMeaning\":" + quote(labels[5]) + ",\"syncUncertaintyNanos\":" + codec.definition.syncUncertaintyNanos() + "}";
                write(definitions, row);
            } else if (codec.validObservation(buffer, body, size)) {
                if (event.sourceConfigVersion() != source.configuration || event.clockId() != source.clock
                    || event.receiveTimeNanos() < 0 || event.receiveTimeNanos() > event.completeTimeNanos()
                    || event.completeTimeNanos() > event.readyTimeNanos() || codec.observation.rawFrameSequence() != source.rawSequence + 1
                    || codec.observation.subeventIndex() != 0 || codec.nativeLength != 0
                    || codec.observation.nativeNumericId() != CaptureCodec.UNKNOWN_NATIVE_ID) {
                    throw new IllegalStateException("invalid raw frame binding/sequence/times/native encoding");
                }
                source.rawSequence++; source.frames++; source.bytes += codec.rawLength;
                final String raw;
                if (source.id == 7) { raw = "\"raw\":" + quote(utf8(buffer, codec.rawOffset, codec.rawLength)); }
                else {
                    if (codec.rawLength < 1 || buffer.getByte(codec.rawOffset) < 0 || buffer.getByte(codec.rawOffset) > 1) {
                        throw new IllegalStateException("invalid binary/text transport tag");
                    }
                    final byte[] bytes = new byte[codec.rawLength]; buffer.getBytes(codec.rawOffset, bytes);
                    raw = "\"rawBase64\":" + quote(Base64.getEncoder().encodeToString(bytes));
                }
                row = "{\"kind\":\"observation\"," + metadata + ",\"session\":" + quote(source.session)
                    + ",\"rawFrameSequence\":" + codec.observation.rawFrameSequence() + ",\"subeventIndex\":0,\"rawByteLength\":" + codec.rawLength
                    + ",\"nativeNumericId\":" + quote(Long.toUnsignedString(codec.observation.nativeNumericId())) + "," + raw + "}";
            } else { throw new IllegalStateException("unknown capture payload"); }
            write(output, row);
            sequence = event.sequence();
        }
    }

    private static int next(final long value) {
        if (value >= 0xFFFF_FFFEL) { throw new IllegalStateException("source binding exhausted"); }
        return (int)(value + 1);
    }
    private static URI localEndpoint(final String property) {
        final URI endpoint = URI.create(System.getProperty(property, ""));
        if (!"ws".equals(endpoint.getScheme()) || !"127.0.0.1".equals(endpoint.getHost())
            || endpoint.getPort() < 1 || endpoint.getPort() > 65535 || !"/ws".equals(endpoint.getPath())
            || endpoint.getRawQuery() != null || endpoint.getRawFragment() != null || endpoint.getRawUserInfo() != null) {
            throw new IllegalArgumentException("local synthetic endpoint must be a loopback /ws peer");
        }
        return endpoint;
    }
    private static String sha256(final Path file) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
    }
    private static String utf8(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] bytes = new byte[length]; buffer.getBytes(offset, bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (final java.nio.charset.CharacterCodingException invalid) { throw new IllegalStateException("invalid captured UTF-8", invalid); }
    }
    private static void write(final BufferedWriter writer, final String row) {
        try { writer.write(row); writer.newLine(); } catch (final IOException failure) { throw new UncheckedIOException(failure); }
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
