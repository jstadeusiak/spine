package spine.sequencer;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.jar.JarFile;

/** Repeatable local protocol/capture/restart acceptance. Synthetic timing is never a venue benchmark. */
public final class BinancePairLocalSmoke {
    static final int BATCHES = 6, TRADES_PER_BATCH = 2, TRADES = BATCHES * TRADES_PER_BATCH;
    static final int JSON_FRAMES = 1 + TRADES, SBE_FRAMES = 1 + BATCHES;
    private static final byte[] ACK = "{\"result\":null,\"id\":1}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SUBSCRIPTION = "{\"method\":\"SUBSCRIBE\",\"params\":[\"btcusdt@trade\"],\"id\":1}".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PING = {(byte)0xFE, 0, 42, 7, 8};

    public static void main(final String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) { throw new IllegalArgumentException("DATA_DIR BUNDLE_JAR [PYTHON=python3]"); }
        final Path data = Path.of(args[0]).toAbsolutePath(), bundle = Path.of(args[1]).toAbsolutePath();
        final String python = args.length == 3 ? args[2] : "python3";
        require(Files.isRegularFile(bundle), "retained runnable bundle required");
        require(!Files.exists(data.resolve("local-expected.json")) && !Files.exists(data.resolve("state")),
            "use an unused data directory; previous attempts must be preserved");
        Files.createDirectories(data);
        try {
            missingKey(data, bundle, null, "missing-key");
            missingKey(data, bundle, " ", "blank-key");
            final List<String> expectedRuns = new ArrayList<>();
            for (int clock = 1; clock <= 2; clock++) {
                final Corpus corpus = new Corpus(clock);
                final CountDownLatch subscribed = new CountDownLatch(2), release = new CountDownLatch(1);
                try (Peer json = new Peer(false, corpus, subscribed, release);
                     Peer sbe = new Peer(true, corpus, subscribed, release)) {
                    expectedRuns.add(corpus.manifest(clock, json.uri(), sbe.uri()));
                    Files.writeString(data.resolve("local-expected.json"), "{\"provenance\":"
                        + quote(BinancePairSmoke.LOCAL_PROVENANCE) + ",\"runs\":[" + String.join(",", expectedRuns) + "]}\n");
                    final List<String> command = javaCommand(bundle);
                    command.add("-Dspine.test.localSynthetic=true");
                    command.add("-Dspine.test.jsonEndpoint=" + json.uri());
                    command.add("-Dspine.test.sbeEndpoint=" + sbe.uri());
                    command.add(BinancePairSmoke.class.getName()); command.add(data.toString()); command.add("2");
                    final Process child = process(command, data, "child-" + clock, BinancePairSmoke.localCredential());
                    try {
                        final Path ready = data.resolve("local-ready-" + clock + ".json");
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
                        while (!Files.isRegularFile(ready) || subscribed.getCount() != 0) {
                            json.healthy(); sbe.healthy();
                            require(child.isAlive(), "paired child stopped before both subscriptions were ready; inspect retained logs");
                            require(System.nanoTime() - deadline < 0, "paired child readiness timeout");
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                        }
                        require(Files.readString(ready).equals("{\"clockId\":" + clock + "}\n"), "child clock readiness differs");
                        release.countDown();
                        require(child.waitFor(90, TimeUnit.SECONDS), "paired child completion timeout");
                        require(child.exitValue() == 0, "paired child failed; inspect retained stdout/stderr");
                        require(json.done.await(5, TimeUnit.SECONDS) && sbe.done.await(5, TimeUnit.SECONDS), "scripted peers did not finish corpus");
                        json.healthy(); sbe.healthy();
                        require(json.pong && sbe.pong, "RFC ping payload did not return as pong");
                        Files.writeString(data.resolve("peer-" + clock + ".json"), "{\"bothSubscribed\":true,\"jsonPong\":true,"
                            + "\"sbePong\":true,\"jsonApplicationFrames\":" + JSON_FRAMES + ",\"sbeApplicationFrames\":" + SBE_FRAMES + "}\n");
                    } finally {
                        release.countDown();
                        if (child.isAlive()) { child.destroyForcibly(); child.waitFor(10, TimeUnit.SECONDS); }
                    }
                }
            }
            final Path research = extractResearch(bundle, data);
            final List<String> audit = List.of(python, research.resolve("binance_pair_audit.py").toString(), "--local-synthetic", data.toString());
            finish(process(audit, data, "audit-first", null), "first strict local cold audit");
            final byte[] first = Files.readAllBytes(data.resolve("pair-audit.json"));
            Files.write(data.resolve("pair-audit-first.json"), first);
            finish(process(audit, data, "audit-second", null), "second strict local cold audit");
            require(Arrays.equals(first, Files.readAllBytes(data.resolve("pair-audit.json"))), "cold audit reports differ byte for byte");
            final String result = new String(first, StandardCharsets.UTF_8);
            require(result.contains("\"status\": \"passed-local-synthetic\"") && result.contains("\"payloadMismatchCount\": 0")
                && result.contains("\"expectedRawVerified\": true") && result.contains("\"expectedTradeOrderVerified\": true")
                && result.contains("\"expectedPayloadsEquivalent\": true"), "local audit omitted its exact corpus proof");
            require(occurrences(result, "\"payloadEquivalentUniqueTrades\": " + TRADES) == 2, "expected matched corpus count differs");
            credentialAbsent(data, BinancePairSmoke.localCredential().getBytes(StandardCharsets.UTF_8));
            final String acceptance = "{\"status\":\"passed-local-synthetic\",\"venueData\":false,\"runs\":2,\"tradesPerSourcePerRun\":"
                + TRADES + ",\"exactRawAndWireOrder\":true,\"restartPrefixVerified\":true,\"coldAuditReportsIdentical\":true,"
                + "\"missingAndBlankKeyHaveNoSideEffects\":true,\"credentialRetained\":false,\"latencyIsVenueBenchmark\":false}\n";
            Files.writeString(data.resolve("local-acceptance.json"), acceptance);
            System.out.print(acceptance);
        } catch (final Exception | Error failure) {
            final StringWriter trace = new StringWriter(); failure.printStackTrace(new PrintWriter(trace));
            Files.writeString(data.resolve("local-acceptance-failure.txt"), trace.toString());
            throw failure;
        }
    }

    private static void missingKey(final Path data, final Path bundle, final String key, final String name) throws Exception {
        final Path rejected = data.resolve(name + "-data");
        final List<String> command = javaCommand(bundle);
        command.add(BinancePairSmoke.class.getName()); command.add(rejected.toString()); command.add("2");
        final Process child = process(command, data, name, key);
        try {
            require(child.waitFor(30, TimeUnit.SECONDS), "missing-key guard timeout");
            require(child.exitValue() != 0 && !Files.exists(rejected)
                && Files.readString(data.resolve(name + ".stderr.log")).contains("BINANCE_SBE_API_KEY required"),
                "missing/blank key guard did not precede side effects");
        } finally { if (child.isAlive()) { child.destroyForcibly(); child.waitFor(10, TimeUnit.SECONDS); } }
    }

    private static List<String> javaCommand(final Path bundle) {
        return new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens", "java.base/java.util.zip=ALL-UNNAMED",
            "-cp", bundle.toString()));
    }

    private static Process process(final List<String> command, final Path data, final String name, final String key) throws Exception {
        final ProcessBuilder builder = new ProcessBuilder(command).redirectOutput(data.resolve(name + ".stdout.log").toFile())
            .redirectError(data.resolve(name + ".stderr.log").toFile());
        if (key == null) { builder.environment().remove("BINANCE_SBE_API_KEY"); }
        else { builder.environment().put("BINANCE_SBE_API_KEY", key); }
        return builder.start();
    }

    private static void finish(final Process process, final String description) throws Exception {
        try {
            require(process.waitFor(60, TimeUnit.SECONDS), description + " timeout");
            require(process.exitValue() == 0, description + " failed; inspect retained logs");
        } finally { if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); } }
    }

    private static Path extractResearch(final Path bundle, final Path data) throws Exception {
        final Path research = data.resolve("research");
        try (JarFile jar = new JarFile(bundle.toFile())) {
            for (final String name : List.of("binance_pair_audit.py", "binance_json_audit.py", "fixtures/upstream/stream_1_0.xml")) {
                final var entry = jar.getJarEntry("research/" + name);
                require(entry != null, "retained bundle lacks cold audit/schema resource");
                final Path destination = research.resolve(name);
                Files.createDirectories(destination.getParent());
                try (InputStream input = jar.getInputStream(entry)) { Files.copy(input, destination, StandardCopyOption.REPLACE_EXISTING); }
            }
        }
        return research;
    }

    private static void credentialAbsent(final Path data, final byte[] key) throws Exception {
        try (var paths = Files.walk(data)) {
            for (final Path file : paths.filter(Files::isRegularFile).toList()) {
                require(!contains(Files.readAllBytes(file), key), "dummy credential retained in data/artifacts");
                if (file.toString().endsWith(".jar")) {
                    try (JarFile jar = new JarFile(file.toFile())) {
                        for (final var entry : jar.stream().filter(item -> !item.isDirectory()).toList()) {
                            try (InputStream input = jar.getInputStream(entry)) {
                                require(!contains(input.readAllBytes(), key), "dummy credential retained in executable entry");
                            }
                        }
                    }
                }
            }
        }
    }

    private static boolean contains(final byte[] bytes, final byte[] sought) {
        outer: for (int offset = 0; offset <= bytes.length - sought.length; offset++) {
            for (int i = 0; i < sought.length; i++) { if (bytes[offset + i] != sought[i]) { continue outer; } }
            return true;
        }
        return false;
    }

    private static int occurrences(final String text, final String value) {
        int count = 0, offset = 0;
        while ((offset = text.indexOf(value, offset)) >= 0) { count++; offset += value.length(); }
        return count;
    }

    private static final class Corpus {
        final List<byte[]> json = new ArrayList<>(), binary = new ArrayList<>();
        final List<Long> ids = new ArrayList<>();
        Corpus(final int round) {
            for (int batch = 0; batch < BATCHES; batch++) {
                final long event = 1_700_000_000_000_000L + round * 1_000_000L + batch * 1000L, transaction = event - 7;
                final ByteBuffer frame = ByteBuffer.allocate(8 + 18 + 6 + TRADES_PER_BATCH * 25 + 1 + 7).order(ByteOrder.LITTLE_ENDIAN);
                frame.putShort((short)18).putShort((short)10000).putShort((short)1).putShort((short)0)
                    .putLong(event).putLong(transaction).put((byte)-18).put((byte)-9).putShort((short)25).putInt(TRADES_PER_BATCH);
                for (int child = 0; child < TRADES_PER_BATCH; child++) {
                    final int index = batch * TRADES_PER_BATCH + child;
                    final long id = 9_007_199_254_741_000L + round * 100L + index;
                    final long price = 1_234_567_890_123_456_789L + index, quantity = 123_456_789L + index;
                    final boolean maker = (index & 1) == 1;
                    ids.add(id);
                    frame.putLong(id).putLong(price).putLong(quantity).put((byte)(maker ? 1 : 0));
                    json.add(("{\"e\":\"trade\",\"E\":" + event + ",\"s\":\"BTCUSDT\",\"t\":" + id
                        + ",\"p\":" + quote(BigDecimal.valueOf(price, 18).toPlainString())
                        + ",\"q\":" + quote(BigDecimal.valueOf(quantity, 9).toPlainString()) + ",\"T\":" + transaction
                        + ",\"m\":" + maker + ",\"M\":true}").getBytes(StandardCharsets.UTF_8));
                }
                frame.put((byte)7).put("BTCUSDT".getBytes(StandardCharsets.UTF_8));
                require(!frame.hasRemaining(), "synthetic SBE layout size differs");
                binary.add(frame.array());
            }
        }
        String manifest(final int clock, final URI jsonEndpoint, final URI sbeEndpoint) {
            return "{\"clockId\":" + clock + ",\"jsonEndpoint\":" + quote(jsonEndpoint.toString()) + ",\"sbeEndpoint\":"
                + quote(sbeEndpoint.toString()) + ",\"expectedMatchedTrades\":" + TRADES + ",\"sources\":["
                + expected(7, false) + "," + expected(8, true) + "]}";
        }
        String expected(final int source, final boolean tagged) {
            final List<String> raw = new ArrayList<>();
            raw.add(quote(Base64.getEncoder().encodeToString(tagged ? tagged(0, ACK) : ACK)));
            for (final byte[] frame : tagged ? binary : json) {
                raw.add(quote(Base64.getEncoder().encodeToString(tagged ? tagged(1, frame) : frame)));
            }
            return "{\"sourceId\":" + source + ",\"rawBase64\":[" + String.join(",", raw) + "],\"tradeIds\":" + ids + "}";
        }
        private static byte[] tagged(final int tag, final byte[] body) {
            final byte[] bytes = new byte[body.length + 1]; bytes[0] = (byte)tag;
            System.arraycopy(body, 0, bytes, 1, body.length); return bytes;
        }
    }

    private static final class Peer implements AutoCloseable {
        final ServerSocket listener;
        final CountDownLatch stop = new CountDownLatch(1), done = new CountDownLatch(1);
        final Thread thread;
        volatile Socket socket;
        volatile Throwable failure;
        volatile boolean pong;
        Peer(final boolean sbe, final Corpus corpus, final CountDownLatch subscribed, final CountDownLatch release) throws Exception {
            listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")); listener.setSoTimeout(60_000);
            thread = Thread.ofPlatform().name(sbe ? "local-sbe-peer" : "local-json-peer").start(() -> {
                try (Socket connected = listener.accept()) {
                    socket = connected; connected.setSoTimeout(30_000);
                    final InputStream input = connected.getInputStream(); final OutputStream output = connected.getOutputStream();
                    final String request = headers(input);
                    require(request.startsWith("GET /ws HTTP/1.1\r\n"), "unexpected local WebSocket path");
                    String upgradeKey = null, credential = null;
                    for (final String line : request.split("\r\n")) {
                        final int colon = line.indexOf(':');
                        if (colon > 0 && line.substring(0, colon).equalsIgnoreCase("Sec-WebSocket-Key")) { upgradeKey = line.substring(colon + 1).trim(); }
                        if (colon > 0 && line.substring(0, colon).equalsIgnoreCase("X-MBX-APIKEY")) { credential = line.substring(colon + 1).trim(); }
                    }
                    require(upgradeKey != null && (sbe ? BinancePairSmoke.localCredential().equals(credential) : credential == null),
                        "local credential boundary differs");
                    final String accept = Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-1")
                        .digest((upgradeKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)));
                    output.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                        + accept + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII)); output.flush();
                    require(Arrays.equals(readFrame(input, 1), SUBSCRIPTION), "subscription bytes differ");
                    send(output, true, 1, ACK, 0, ACK.length); subscribed.countDown();
                    require(release.await(90, TimeUnit.SECONDS), "paired corpus release timeout");
                    final long start = System.nanoTime();
                    for (int batch = 0; batch < BATCHES; batch++) {
                        final long scheduled = start + TimeUnit.MILLISECONDS.toNanos(250L * batch);
                        while (System.nanoTime() - scheduled < 0) { LockSupport.parkNanos(Math.min(1_000_000, scheduled - System.nanoTime())); }
                        final List<byte[]> frames = sbe ? List.of(corpus.binary.get(batch))
                            : corpus.json.subList(batch * TRADES_PER_BATCH, (batch + 1) * TRADES_PER_BATCH);
                        for (int index = 0; index < frames.size(); index++) {
                            final byte[] frame = frames.get(index);
                            if (batch == 0 && index == 0) {
                                final int split = sbe ? 17 : 11;
                                send(output, false, sbe ? 2 : 1, frame, 0, split);
                                send(output, true, 9, PING, 0, PING.length);
                                send(output, true, 0, frame, split, frame.length - split);
                            } else { send(output, true, sbe ? 2 : 1, frame, 0, frame.length); }
                        }
                        if (batch == 0) { require(Arrays.equals(readFrame(input, 10), PING), "RFC pong payload differs"); pong = true; }
                    }
                    done.countDown(); stop.await(90, TimeUnit.SECONDS);
                } catch (final Throwable problem) { failure = problem; }
            });
        }
        URI uri() { return URI.create("ws://127.0.0.1:" + listener.getLocalPort() + "/ws"); }
        void healthy() { if (failure != null) { throw new AssertionError("scripted WebSocket peer failed", failure); } }
        @Override public void close() throws Exception {
            stop.countDown(); listener.close(); if (socket != null) { socket.close(); }
            thread.join(10_000); require(!thread.isAlive(), "scripted peer did not stop"); healthy();
        }
        private static String headers(final InputStream input) throws Exception {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream(); int tail = 0;
            while (bytes.size() < 16_384) {
                final int value = octet(input); bytes.write(value); tail = tail << 8 | value;
                if (tail == 0x0D0A0D0A) { return bytes.toString(StandardCharsets.US_ASCII); }
            }
            throw new IllegalStateException("oversized local handshake");
        }
        private static byte[] readFrame(final InputStream input, final int opcode) throws Exception {
            final int first = octet(input), second = octet(input);
            require(first == (0x80 | opcode) && (second & 0x80) != 0, "unexpected client frame flags/opcode/mask");
            long length = second & 0x7F;
            if (length == 126) { length = octet(input) << 8 | octet(input); }
            else if (length == 127) { length = 0; for (int i = 0; i < 8; i++) { length = length << 8 | octet(input); } }
            require(length >= 0 && length <= 65_500, "client frame length exceeds bound");
            final byte[] mask = input.readNBytes(4), payload = input.readNBytes((int)length);
            require(mask.length == 4 && payload.length == length, "truncated client frame");
            for (int i = 0; i < payload.length; i++) { payload[i] ^= mask[i & 3]; }
            return payload;
        }
        private static void send(final OutputStream output, final boolean last, final int opcode,
            final byte[] bytes, final int offset, final int length) throws Exception {
            output.write((last ? 0x80 : 0) | opcode);
            if (length < 126) { output.write(length); }
            else { output.write(126); output.write(length >>> 8); output.write(length & 255); }
            output.write(bytes, offset, length); output.flush();
        }
        private static int octet(final InputStream input) throws Exception {
            final int value = input.read(); if (value < 0) { throw new EOFException("local peer disconnected"); } return value;
        }
    }

    private static void require(final boolean condition, final String message) { if (!condition) { throw new IllegalStateException(message); } }
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
