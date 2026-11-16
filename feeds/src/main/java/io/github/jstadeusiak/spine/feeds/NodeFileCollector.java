package io.github.jstadeusiak.spine.feeds;

import io.aeron.logbuffer.Header;
import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

/** One retained append-only segment. LF terminates a frame; all preceding bytes, including CR, are preserved. */
public final class NodeFileCollector implements Agent {
    private final FileChannel file;
    private final long clockOrigin;
    private final String session;
    private final byte[] sessionBytes;
    private final ByteBuffer input = ByteBuffer.allocateDirect(4096);
    private final UnsafeBuffer line = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES - CaptureCodec.OBSERVATION_OVERHEAD]);
    private final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
    private final CaptureCodec codec = new CaptureCodec();
    private final Wire wire = new Wire();
    private CapturedIngress ingress;
    private long position, frameSequence, readTime, receiveTime, completeTime, recoveredConfiguration, recoveredClock;
    private int lineLength, readWork, configuration, clock;
    private boolean complete, definition, failed;

    public NodeFileCollector(final Path path, final long clockOrigin) throws IOException {
        final Path real = path.toRealPath();
        final BasicFileAttributes attributes = Files.readAttributes(real, BasicFileAttributes.class);
        if (!attributes.isRegularFile() || attributes.fileKey() == null) {
            throw new IOException("retained regular segment with filesystem identity required");
        }
        session = real + "#" + attributes.fileKey();
        sessionBytes = session.getBytes(StandardCharsets.UTF_8);
        if (sessionBytes.length > CaptureCodec.MAX_LABEL_BYTES) { throw new IOException("segment session exceeds label bound"); }
        final FileChannel opened = FileChannel.open(real, StandardOpenOption.READ);
        try {
            if (!attributes.fileKey().equals(Files.readAttributes(real, BasicFileAttributes.class).fileKey())) {
                throw new IOException("segment changed while opening");
            }
        } catch (final IOException | RuntimeException failure) { opened.close(); throw failure; }
        file = opened;
        this.clockOrigin = clockOrigin;
        input.limit(0);
    }

    public String session() { return session; }

    /** Cold capture replay verifies the entire retained raw prefix, including definitions binding this segment. */
    public void recovered(final DirectBuffer proposal, final int offset, final int length, final Header header) {
        if (ingress != null || !wire.validProposal(proposal, offset, length)) {
            throw new IllegalStateException("invalid or late segment recovery");
        }
        final int body = offset + Wire.PROPOSAL_OVERHEAD, bytes = length - Wire.PROPOSAL_OVERHEAD;
        if (codec.validDefinition(proposal, body, bytes)) {
            if (codec.labelLength(2) != sessionBytes.length) { throw new IllegalStateException("capture belongs to another segment"); }
            for (int i = 0; i < sessionBytes.length; i++) {
                if (proposal.getByte(codec.labelOffset(2) + i) != sessionBytes[i]) {
                    throw new IllegalStateException("capture belongs to another segment");
                }
            }
            recoveredConfiguration = wire.proposal.sourceConfigVersion();
            recoveredClock = wire.proposal.clockId();
            definition = true;
            return;
        }
        if (!definition || wire.proposal.sourceConfigVersion() != recoveredConfiguration
            || wire.proposal.clockId() != recoveredClock || !codec.validObservation(proposal, body, bytes)
            || frameSequence == Long.MAX_VALUE || codec.observation.rawFrameSequence() != frameSequence + 1
            || codec.observation.subeventIndex() != 0 || codec.nativeLength != 0
            || codec.observation.nativeNumericId() != CaptureCodec.UNKNOWN_NATIVE_ID) {
            throw new IllegalStateException("invalid segment capture lineage");
        }
        try {
            while (!readLine()) {
                if (readWork == 0) { throw fail("captured prefix missing from retained segment"); }
            }
            if (codec.rawLength != lineLength) { throw fail("captured prefix differs from retained segment"); }
            for (int i = 0; i < lineLength; i++) {
                if (proposal.getByte(codec.rawOffset + i) != line.getByte(i)) {
                    throw fail("captured prefix differs from retained segment");
                }
            }
            consume();
        } catch (final IOException failure) { throw new UncheckedIOException(failure); }
    }

    /** Attach after cold recovery and admission of the current source definition. Ingress remains caller-owned. */
    public void attach(final CapturedIngress ingress, final int configuration, final int clock) {
        if (this.ingress != null || ingress == null || configuration == 0 || configuration == -1 || clock == 0 || clock == -1
            || !ingress.idle() || ingress.lastConfiguration() != Integer.toUnsignedLong(configuration)
            || ingress.lastClockId() != Integer.toUnsignedLong(clock)) {
            throw new IllegalStateException("admitted definition required before segment attachment");
        }
        this.ingress = ingress;
        this.configuration = configuration;
        this.clock = clock;
    }

    @Override public int doWork() throws IOException {
        if (ingress == null) { throw new IllegalStateException("attach admitted ingress"); }
        final int work = ingress.poll();
        if (!ingress.idle()) { return work; }
        if (!readLine()) { return work + readWork; }
        if (frameSequence == Long.MAX_VALUE) { throw new IllegalStateException("segment frame sequence exhausted"); }
        final int length = codec.encodeObservation(payload, 0, frameSequence + 1, 0,
            CaptureCodec.UNKNOWN_NATIVE_ID, null, 0, 0, line, 0, lineLength);
        if (!ingress.submit(configuration, clock, receiveTime, completeTime, now(), payload, 0, length)) {
            throw new IllegalStateException("idle segment ingress refused valid frame");
        }
        consume();
        return work + 1;
    }

    /** At most one fixed-size source read per call; incomplete EOF remains owned until LF arrives. */
    boolean readLine() throws IOException {
        readWork = 0;
        if (failed) { throw new IOException("segment collector stopped"); }
        if (file.size() < position) { throw fail("retained segment was truncated"); }
        if (complete) { return true; }
        if (!input.hasRemaining()) {
            input.clear();
            final int read = file.read(input, position);
            input.flip();
            if (read <= 0) { return false; }
            position += read;
            readTime = now();
        }
        while (input.hasRemaining()) {
            readWork = 1;
            if (lineLength == 0) { receiveTime = readTime; }
            final byte value = input.get();
            if (value == '\n') { completeTime = now(); complete = true; return true; }
            if (lineLength == line.capacity()) { throw fail("segment frame exceeds capture bound"); }
            line.putByte(lineLength++, value);
        }
        return false;
    }

    private void consume() { frameSequence++; lineLength = 0; complete = false; }
    private IOException fail(final String message) { failed = true; return new IOException(message); }
    private long now() {
        final long time = System.nanoTime() - clockOrigin;
        if (time < 0) { throw new IllegalStateException("clock origin is outside this process interval"); }
        return time;
    }
    @Override public String roleName() { return "node-file-collector"; }
    @Override public void onClose() { CloseHelper.close(file); }
}
