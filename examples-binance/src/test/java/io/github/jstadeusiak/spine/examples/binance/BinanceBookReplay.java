package io.github.jstadeusiak.spine.examples.binance;

import io.github.jstadeusiak.spine.sequencer.SequencerCore;

import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.market.sbe.BookSourceEncoder;
import io.github.jstadeusiak.spine.protocol.market.sbe.BootstrapChunkEncoder;
import io.github.jstadeusiak.spine.protocol.market.sbe.MessageHeaderEncoder;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/** Cold real-data oracle/recovery acceptance and isolated warmed core processing measurement. */
public final class BinanceBookReplay {
    private static final long LOG = 42, PRODUCER = 100;
    private record Command(UnsafeBuffer buffer, long loggedTime, int source, String kind) { }
    private record Corpus(List<Command> commands, int observationStart) { }

    public static void main(final String[] args) throws Exception {
        if (args.length < 1 || args.length > 3 || (args.length == 3 && !args[2].equals("startup"))) {
            throw new IllegalArgumentException("FIXTURE_DIR [REPLAYS=400] [startup]");
        }
        final Path directory = Path.of(args[0]).toAbsolutePath();
        final int replays = args.length >= 2 ? Integer.parseInt(args[1]) : 400;
        if (replays < 1 || replays > 2000) { throw new IllegalArgumentException("replays must be 1..2000"); }
        final Corpus corpus = load(directory);
        final byte[] startupSnapshot = args.length == 3 ? startup(directory, corpus, replays) : null;
        final SequencerCore core = initialized();
        final byte[][] canonical = new byte[corpus.commands.size()][];
        byte[] seed = null;
        try (BufferedWriter output = Files.newBufferedWriter(directory.resolve("actual.jsonl"))) {
            for (int i = 0; i < corpus.commands.size(); i++) {
                final Command command = corpus.commands.get(i);
                apply(core, command);
                checkObservation(core, command);
                canonical[i] = bytes(core.resultBuffer(), core.eventOffset(), core.eventLength());
                output.write(state(core, command, i));
                output.newLine();
                if (i + 1 == corpus.observationStart) { seed = snapshot(core); }
            }
        }
        final byte[] finalSnapshot = snapshot(core);
        if (startupSnapshot != null && !Arrays.equals(startupSnapshot, finalSnapshot)) {
            throw new IllegalStateException("Startup diagnostic differs from the complete correctness replay");
        }
        final TreeSet<Integer> cuts = new TreeSet<>(List.of(0, 1, 3, 4, 6, corpus.observationStart - 1,
            corpus.observationStart, corpus.observationStart + 1, corpus.commands.size() / 4,
            corpus.commands.size() / 2, corpus.commands.size() * 3 / 4, corpus.commands.size()));
        for (final int cut : cuts) {
            final SequencerCore prefix = initialized();
            for (int i = 0; i < cut; i++) { apply(prefix, corpus.commands.get(i)); }
            final byte[] checkpoint = snapshot(prefix);
            final SequencerCore restored = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
            restored.restore(new UnsafeBuffer(checkpoint), 0, checkpoint.length);
            for (int i = cut; i < corpus.commands.size(); i++) {
                apply(restored, corpus.commands.get(i));
                if (!Arrays.equals(canonical[i], bytes(restored.resultBuffer(), restored.eventOffset(), restored.eventLength()))) {
                    throw new IllegalStateException("Canonical suffix differs after checkpoint cut " + cut + " at command " + i);
                }
            }
            if (!Arrays.equals(finalSnapshot, snapshot(restored))) {
                throw new IllegalStateException("Complete final state differs after checkpoint cut " + cut);
            }
        }
        if (seed == null) { throw new IllegalStateException("Missing complete bootstrap seed"); }
        final String profile = profile(corpus, seed, replays, ((BinanceApplication)core.application()).book.nativeFrontier);
        Files.writeString(directory.resolve("profile.json"), profile + "\n");
        final String report = "{\"commands\":" + corpus.commands.size() + ",\"applicationFrames\":"
            + (corpus.commands.size() - corpus.observationStart) + ",\"snapshotRecoveryPassed\":true,\"snapshotRecoveryCuts\":"
            + cuts + ",\"borrowedObservationPreserved\":true,\"canonicalSuffixBytesIdentical\":true,\"finalSnapshotBytes\":"
            + finalSnapshot.length + ",\"finalSnapshotSha256\":\"" + sha(finalSnapshot) + "\"}";
        Files.writeString(directory.resolve("replay.json"), report + "\n");
        System.out.println(report);
        System.out.println(profile);
    }

    private static Corpus load(final Path directory) throws Exception {
        final List<Command> commands = new ArrayList<>();
        final Wire wire = new Wire();
        final CaptureCodec capture = new CaptureCodec();
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        for (int source = 1; source <= 3; source++) {
            final BookSourceEncoder binding = new BookSourceEncoder().wrapAndApplyHeader(payload, 0, header)
                .venueId(1).instrumentId(1).epoch(1).format((short)source).priceScale((short)8)
                .quantityScale((short)8).maximumAgeNanos(1_000_000_000L);
            add(commands, wire, payload, 8 + binding.encodedLength(), source, 0, 0, "binding");
        }
        final byte[] snapshot = Files.readAllBytes(directory.resolve("snapshot.json"));
        final UnsafeBuffer raw = new UnsafeBuffer(snapshot);
        final long bootstrapTime = Long.parseLong(Files.readString(directory.resolve("bootstrap-time.txt")).strip());
        for (int offset = 0; offset < snapshot.length; offset += 60000) {
            final int length = Math.min(60000, snapshot.length - offset);
            final BootstrapChunkEncoder chunk = new BootstrapChunkEncoder().wrapAndApplyHeader(payload, 0, header)
                .bootstrapId(1).totalLength(snapshot.length).chunkOffset(offset).putFragment(raw, offset, length);
            add(commands, wire, payload, 8 + chunk.encodedLength(), 3, bootstrapTime, bootstrapTime, "bootstrap");
        }
        final int observationStart = commands.size();
        for (final String line : Files.readAllLines(directory.resolve("frames.tsv"))) {
            final String[] fields = line.split("\t", -1);
            if (fields.length != 6) { throw new IllegalArgumentException("Invalid retained frame fixture row"); }
            final int source = Integer.parseInt(fields[0]);
            final long receive = Long.parseLong(fields[1]), loggedTime = Long.parseLong(fields[2]);
            final long frame = Long.parseLong(fields[3]);
            final byte[] original = Base64.getDecoder().decode(fields[5]);
            final byte[] framed;
            if (source == 2) {
                framed = new byte[original.length + 1];
                framed[0] = (byte)(fields[4].equals("depth") ? 1 : 0);
                System.arraycopy(original, 0, framed, 1, original.length);
            } else { framed = original; }
            final UnsafeBuffer frameBytes = new UnsafeBuffer(framed);
            final int length = capture.encodeObservation(payload, 0, frame, 0, CaptureCodec.UNKNOWN_NATIVE_ID,
                null, 0, 0, frameBytes, 0, frameBytes.capacity());
            add(commands, wire, payload, length, source, receive, loggedTime, fields[4]);
        }
        return new Corpus(commands, observationStart);
    }

    private static void add(final List<Command> commands, final Wire wire, final DirectBuffer payload,
        final int length, final int source, final long receive, final long loggedTime, final String kind) {
        final UnsafeBuffer command = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + length]);
        wire.encodeProposal(command, 0, LOG, PRODUCER, commands.size() + 1L, source, 1, 1,
            receive, receive, receive, payload, 0, length);
        commands.add(new Command(command, loggedTime, source, kind));
    }

    private static SequencerCore initialized() {
        final SequencerCore core = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        final Wire wire = new Wire();
        final UnsafeBuffer initialize = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        if (core.onCommand(initialize, 0, wire.encodeInitialize(initialize, 0, LOG, BinanceApplication.IDENTITY), 0, 1) != AdmissionStatus.INITIALIZED) {
            throw new IllegalStateException("Initialization rejected");
        }
        return core;
    }

    private static void apply(final SequencerCore core, final Command command) {
        if (core.onCommand(command.buffer, 0, command.buffer.capacity(), command.loggedTime, 1) != AdmissionStatus.ACCEPTED) {
            throw new IllegalStateException("Captured command rejected");
        }
    }

    private static void checkObservation(final SequencerCore core, final Command command) {
        final Wire wire = new Wire();
        if (!wire.validResult(core.resultBuffer(), 0, core.resultLength())) {
            throw new IllegalStateException("Invalid canonical result framing");
        }
        final int offset = core.eventOffset() + Wire.EVENT_OVERHEAD;
        final DirectBuffer output = core.resultBuffer();
        if (output.getShort(offset, Wire.ORDER) != 84 || output.getShort(offset + 2, Wire.ORDER) != 3
            || output.getShort(offset + 4, Wire.ORDER) != 401 || output.getShort(offset + 6, Wire.ORDER) != 1) {
            throw new IllegalStateException("Market result schema differs");
        }
        final int original = command.buffer.capacity() - Wire.PROPOSAL_OVERHEAD;
        if (output.getInt(offset + 92, Wire.ORDER) != original) {
            throw new IllegalStateException("Original observation length differs");
        }
        for (int i = 0; i < original; i++) {
            if (output.getByte(offset + 96 + i) != command.buffer.getByte(Wire.PROPOSAL_OVERHEAD + i)) {
                throw new IllegalStateException("Original observation bytes differ");
            }
        }
    }

    private static String state(final SequencerCore core, final Command command, final int index) throws Exception {
        final BinanceBook book = ((BinanceApplication)core.application()).book;
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (int side = 0; side < 2; side++) {
            final Long2LongHashMap map = side == 0 ? book.bids : book.asks;
            final TreeMap<Long, Long> ordered = new TreeMap<>();
            final Long2LongHashMap.EntryIterator iterator = map.entrySet().iterator();
            while (iterator.hasNext()) { iterator.next(); ordered.put(iterator.getLongKey(), iterator.getLongValue()); }
            for (final var row : ordered.entrySet()) {
                digest.update((side + "," + row.getKey() + "," + row.getValue() + "\n").getBytes(StandardCharsets.US_ASCII));
            }
        }
        return "{\"commandIndex\":" + index + ",\"sourceId\":" + command.source + ",\"kind\":\"" + command.kind
            + "\",\"frontier\":" + book.nativeFrontier + ",\"status\":" + book.status + ",\"bidCount\":" + book.bids.size()
            + ",\"askCount\":" + book.asks.size() + ",\"bidPrice\":" + book.bidPrice + ",\"bidSize\":" + book.bidSize
            + ",\"askPrice\":" + book.askPrice + ",\"askSize\":" + book.askSize + ",\"levelsSha256\":\""
            + HexFormat.of().formatHex(digest.digest()) + "\"}";
    }

    private static byte[] snapshot(final SequencerCore core) {
        return bytes(core.resultBuffer(), 0, core.snapshot());
    }

    private static byte[] bytes(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] result = new byte[length];
        buffer.getBytes(offset, result);
        return result;
    }

    private static String sha(final byte[] value) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
    }

    /** Runs before the oracle/recovery replay, on discarded state with no transport or external effects. */
    private static byte[] startup(final Path directory, final Corpus corpus, final int replays) throws Exception {
        final int frames = corpus.commands.size() - corpus.observationStart, warmups = 40;
        final long[] cold = new long[frames], warm = new long[Math.multiplyExact(frames, warmups)];
        final long[] steady = new long[Math.multiplyExact(frames, replays)];
        final RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        final SequencerCore core = initialized();
        for (int i = 0; i < corpus.observationStart; i++) { apply(core, corpus.commands.get(i)); }
        if (((BinanceApplication)core.application()).book.status != BinanceBook.VALID) {
            throw new IllegalStateException("Startup bootstrap is not a valid book");
        }
        final long seedFrontier = ((BinanceApplication)core.application()).book.nativeFrontier;
        final byte[] seed = snapshot(core);
        final UnsafeBuffer seedBuffer = new UnsafeBuffer(seed);
        final long coldStartUptimeMillis = runtime.getUptime();
        startupPass(core, corpus, cold, 0);
        final long coldEndUptimeMillis = runtime.getUptime();
        final long finalFrontier = ((BinanceApplication)core.application()).book.nativeFrontier;
        checkStartupFrontier(core, finalFrontier);
        final byte[] expected = snapshot(core);
        long restoreNanos = 0;
        final long warmStartUptimeMillis = runtime.getUptime();
        final long warmStarted = System.nanoTime();
        for (int repetition = 0; repetition < warmups; repetition++) {
            final long started = System.nanoTime();
            core.restore(seedBuffer, 0, seed.length);
            restoreNanos += System.nanoTime() - started;
            checkStartupFrontier(core, seedFrontier);
            startupPass(core, corpus, warm, repetition * frames);
            checkStartupFrontier(core, finalFrontier);
        }
        final long warmNanos = System.nanoTime() - warmStarted;
        final long warmEndUptimeMillis = runtime.getUptime();
        final long steadyStartUptimeMillis = runtime.getUptime();
        for (int repetition = 0; repetition < replays; repetition++) {
            final long started = System.nanoTime();
            core.restore(seedBuffer, 0, seed.length);
            restoreNanos += System.nanoTime() - started;
            checkStartupFrontier(core, seedFrontier);
            startupPass(core, corpus, steady, repetition * frames);
            checkStartupFrontier(core, finalFrontier);
        }
        final long steadyEndUptimeMillis = runtime.getUptime();
        if (!Arrays.equals(expected, snapshot(core))) {
            throw new IllegalStateException("Startup diagnostic state differs after repeated warm/steady replay");
        }
        try (BufferedWriter output = Files.newBufferedWriter(directory.resolve("startup-samples.csv"))) {
            output.write("phase,repetition,frameIndex,sourceId,kind,nanos\n");
            startupRows(output, corpus, "cold", cold);
            startupRows(output, corpus, "warmup", warm);
            startupRows(output, corpus, "steady", steady);
        }
        final long[] firstPostWarm = Arrays.copyOf(steady, frames);
        final String report = "{\"scope\":\"first retained observation after setup/bootstrap -> SequencerCore.onCommand -> borrowed result bytes\","
            + "\"transportIncluded\":false,\"startupWallTimeMeasured\":false,\"setupRestoreExcluded\":true,\"coldOracleHashingExcluded\":true,"
            + "\"timerOverheadIncluded\":true,\"discardedStateOnly\":true,\"firstPostWarmIncludedInSteady\":true,"
            + "\"phaseUptimeMillis\":{\"coldStart\":" + coldStartUptimeMillis + ",\"coldEnd\":" + coldEndUptimeMillis
            + ",\"warmStart\":" + warmStartUptimeMillis + ",\"warmEnd\":" + warmEndUptimeMillis
            + ",\"steadyStart\":" + steadyStartUptimeMillis + ",\"steadyEnd\":" + steadyEndUptimeMillis + "},"
            + "\"warmupReplays\":" + warmups + ",\"warmupNanos\":" + warmNanos + ",\"warmupIncludesRestore\":true,"
            + "\"measuredReplays\":" + replays + ",\"restoreNanosExcluded\":" + restoreNanos
            + ",\"seedFrontier\":" + seedFrontier + ",\"finalFrontier\":" + finalFrontier
            + ",\"finalSnapshotSha256\":\"" + sha(expected) + "\",\"coldFirstPass\":{" + distribution(cold)
            + "},\"firstPostWarmPass\":{" + distribution(firstPostWarm) + "},\"steady\":{" + distribution(steady) + "}}";
        Files.writeString(directory.resolve("startup.json"), report + "\n");
        System.out.println(report);
        return expected;
    }

    private static void startupPass(final SequencerCore core, final Corpus corpus, final long[] samples,
        final int offset) {
        for (int i = corpus.observationStart; i < corpus.commands.size(); i++) {
            final Command command = corpus.commands.get(i);
            final long started = System.nanoTime();
            apply(core, command);
            samples[offset + i - corpus.observationStart] = System.nanoTime() - started;
        }
    }

    private static void checkStartupFrontier(final SequencerCore core, final long expected) {
        if (((BinanceApplication)core.application()).book.nativeFrontier != expected || ((BinanceApplication)core.application()).book.status != BinanceBook.VALID) {
            throw new IllegalStateException("Startup diagnostic ended at an invalid or unexpected book frontier");
        }
    }

    private static void startupRows(final BufferedWriter output, final Corpus corpus, final String phase,
        final long[] samples) throws Exception {
        final int frames = corpus.commands.size() - corpus.observationStart;
        for (int i = 0; i < samples.length; i++) {
            final Command command = corpus.commands.get(corpus.observationStart + i % frames);
            output.write(phase + "," + i / frames + "," + i % frames + "," + command.source + "," + command.kind
                + "," + samples[i] + "\n");
        }
    }

    private static String profile(final Corpus corpus, final byte[] seed, final int replays, final long finalFrontier) {
        final int frames = corpus.commands.size() - corpus.observationStart;
        final long[] samples = new long[Math.multiplyExact(frames, replays)];
        final long[] sourceSamples = new long[samples.length];
        final int warmups = 40;
        final SequencerCore measured = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        final UnsafeBuffer snapshot = new UnsafeBuffer(seed);
        final long warmStarted = System.nanoTime();
        for (int repetition = 0; repetition < warmups; repetition++) {
            measured.restore(snapshot, 0, seed.length);
            for (int i = corpus.observationStart; i < corpus.commands.size(); i++) { apply(measured, corpus.commands.get(i)); }
        }
        final long warmNanos = System.nanoTime() - warmStarted;
        int sample = 0;
        long restoreNanos = 0, processingNanos = 0;
        for (int repetition = 0; repetition < replays; repetition++) {
            final long restoreStarted = System.nanoTime();
            measured.restore(snapshot, 0, seed.length);
            restoreNanos += System.nanoTime() - restoreStarted;
            for (int i = corpus.observationStart; i < corpus.commands.size(); i++) {
                final Command command = corpus.commands.get(i);
                final long started = System.nanoTime();
                apply(measured, command);
                final long elapsed = System.nanoTime() - started;
                samples[sample] = elapsed;
                sourceSamples[sample++] = command.source;
                processingNanos += elapsed;
            }
            if (((BinanceApplication)measured.application()).book.nativeFrontier != finalFrontier || ((BinanceApplication)measured.application()).book.status != BinanceBook.VALID) {
                throw new IllegalStateException("Measured native flow ended at an invalid book");
            }
        }
        final StringBuilder sources = new StringBuilder("[");
        for (int source = 1; source <= 2; source++) {
            int count = 0;
            for (int i = 0; i < samples.length; i++) { if (sourceSamples[i] == source) { count++; } }
            final long[] values = new long[count];
            for (int i = 0, row = 0; i < samples.length; i++) { if (sourceSamples[i] == source) { values[row++] = samples[i]; } }
            if (source > 1) { sources.append(','); }
            sources.append("{\"sourceId\":").append(source).append(',').append(distribution(values)).append('}');
        }
        sources.append(']');
        return "{\"scope\":\"preencoded observation proposal -> SequencerCore.onCommand -> deterministic recorded-result bytes in borrowed buffer\","
            + "\"transportIncluded\":false,\"liveVenueTimingClaim\":false,\"setupRestoreExcluded\":true,\"coldOracleHashingExcluded\":true,"
            + "\"timerOverheadIncluded\":true,\"warmupReplays\":" + warmups + ",\"warmupNanos\":" + warmNanos
            + ",\"measuredReplays\":" + replays + ",\"restoreNanosExcluded\":" + restoreNanos
            + ",\"processingNanos\":" + processingNanos + ",\"sources\":" + sources + ',' + distribution(samples) + '}';
    }

    private static String distribution(final long[] values) {
        Arrays.sort(values);
        return "\"count\":" + values.length + ",\"p50Ns\":" + percentile(values, 0.5) + ",\"p99Ns\":"
            + percentile(values, 0.99) + ",\"p999Ns\":" + percentile(values, 0.999) + ",\"maxNs\":" + values[values.length - 1];
    }

    private static long percentile(final long[] values, final double percentile) {
        return values[Math.max(0, (int)Math.ceil(values.length * percentile) - 1)];
    }
}
