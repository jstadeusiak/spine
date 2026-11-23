package spine.feeds;

import spine.protocol.CaptureCodec;
import spine.protocol.Wire;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeFileCollectorTest {
    @TempDir Path directory;
    private final CaptureCodec codec = new CaptureCodec();
    private final Wire wire = new Wire();

    @Test void preservesExactLinesIncludingCrEmptyFramesAndRepeatedBlockHeights() throws IOException {
        final Path source = write("{\"block_number\":7}\r\n\n{\"block_number\":7,\"events\":[]}\n");
        final NodeFileCollector collector = new NodeFileCollector(source, System.nanoTime());
        try {
            definition(collector, collector.session(), 1, 1);
            observation(collector, 1, "{\"block_number\":7}\r");
            observation(collector, 2, "");
            observation(collector, 3, "{\"block_number\":7,\"events\":[]}");
            assertThat(collector.readLine()).isFalse();
            assertThat(collector.roleName()).isEqualTo("node-file-collector");
        } finally { collector.onClose(); }
    }

    @Test void holdsIncompleteEofUntilNewlineAndPreservesSplitUtf8Bytes() throws IOException {
        final Path source = directory.resolve("segment");
        final byte[] prefix = new byte[4096];
        Arrays.fill(prefix, (byte)'x');
        prefix[4095] = (byte)0xc3;
        Files.write(source, prefix);
        final NodeFileCollector collector = new NodeFileCollector(source, System.nanoTime());
        try {
            definition(collector, collector.session(), 1, 1);
            assertThat(collector.readLine()).isFalse();
            assertThat(collector.readLine()).isFalse();
            Files.write(source, new byte[]{(byte)0xa9, '\n'}, StandardOpenOption.APPEND);
            assertThat(collector.readLine()).isTrue();
            final byte[] complete = Arrays.copyOf(prefix, prefix.length + 1);
            complete[4096] = (byte)0xa9;
            observation(collector, 1, complete, 1, 1);
            assertThat(collector.readLine()).isFalse();
        } finally { collector.onClose(); }
    }

    @Test void acceptsMaximumRawFrameAndRejectsOneAdditionalByte() throws IOException {
        final byte[] raw = new byte[Wire.MAX_PAYLOAD_BYTES - CaptureCodec.OBSERVATION_OVERHEAD];
        Arrays.fill(raw, (byte)'a');
        final Path source = directory.resolve("segment");
        Files.write(source, Arrays.copyOf(raw, raw.length + 1));
        try (var file = java.nio.channels.FileChannel.open(source, StandardOpenOption.WRITE)) {
            file.write(java.nio.ByteBuffer.wrap(new byte[]{'\n'}), raw.length);
        }
        final NodeFileCollector collector = new NodeFileCollector(source, System.nanoTime());
        try {
            definition(collector, collector.session(), 1, 1);
            observation(collector, 1, raw, 1, 1);
        } finally { collector.onClose(); }
        Files.write(source, raw);
        Files.write(source, new byte[]{'x', '\n'}, StandardOpenOption.APPEND);
        final NodeFileCollector oversized = new NodeFileCollector(source, System.nanoTime());
        try {
            assertThatThrownBy(() -> { while (!oversized.readLine()) { } })
                .isInstanceOf(IOException.class).hasMessageContaining("capture bound");
            assertThatThrownBy(oversized::readLine).isInstanceOf(IOException.class).hasMessageContaining("stopped");
        } finally { oversized.onClose(); }
    }

    @Test void rejectsCorruptionMissingNewlineAndTruncatedSource() throws IOException {
        final Path source = write("changed\n");
        final NodeFileCollector corrupted = new NodeFileCollector(source, System.nanoTime());
        try {
            definition(corrupted, corrupted.session(), 1, 1);
            assertThatThrownBy(() -> observation(corrupted, 1, "original"))
                .isInstanceOf(UncheckedIOException.class).hasMessageContaining("differs");
        } finally { corrupted.onClose(); }
        Files.writeString(source, "partial");
        final NodeFileCollector incomplete = new NodeFileCollector(source, System.nanoTime());
        try {
            definition(incomplete, incomplete.session(), 1, 1);
            assertThatThrownBy(() -> observation(incomplete, 1, "partial"))
                .isInstanceOf(UncheckedIOException.class).hasMessageContaining("missing");
        } finally { incomplete.onClose(); }
        Files.writeString(source, "partial");
        final NodeFileCollector truncated = new NodeFileCollector(source, System.nanoTime());
        try {
            assertThat(truncated.readLine()).isFalse();
            Files.write(source, new byte[0]);
            assertThatThrownBy(truncated::readLine).isInstanceOf(IOException.class).hasMessageContaining("truncated");
        } finally { truncated.onClose(); }
    }

    @Test void replacementAtSamePathNeedsNewSegmentSession() throws IOException {
        final Path source = write("one\n");
        final NodeFileCollector original = new NodeFileCollector(source, System.nanoTime());
        try {
            final String oldSession = original.session();
            Files.move(source, directory.resolve("retained-original"));
            Files.writeString(source, "one\n");
            final NodeFileCollector replacement = new NodeFileCollector(source, System.nanoTime());
            try {
                assertThat(replacement.session()).isNotEqualTo(oldSession);
                assertThatThrownBy(() -> definition(replacement, oldSession, 1, 1))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("another segment");
            } finally { replacement.onClose(); }
        } finally { original.onClose(); }
    }

    @Test void validatesDefinitionsSequencesAndTheirClockBindingWithoutChangingRecordedMetadata() throws IOException {
        final Path source = write("one\ntwo\n");
        final NodeFileCollector collector = new NodeFileCollector(source, System.nanoTime());
        try {
            assertThatThrownBy(() -> observation(collector, 1, "one"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("lineage");
            definition(collector, collector.session(), 1, 1);
            assertThatThrownBy(() -> observation(collector, 2, "one"))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("lineage");
            assertThatThrownBy(() -> observation(collector, 1, "one".getBytes(StandardCharsets.UTF_8), 1, 2))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("lineage");
            final UnsafeBuffer proposal = proposal(1, "one".getBytes(StandardCharsets.UTF_8), 1, 1);
            final byte[] exact = new byte[proposal.capacity()];
            proposal.getBytes(0, exact);
            collector.recovered(proposal, 0, proposal.capacity(), null);
            final byte[] after = new byte[proposal.capacity()];
            proposal.getBytes(0, after);
            assertThat(after).containsExactly(exact);
            assertThat(wire.validProposal(proposal, 0, proposal.capacity())).isTrue();
            assertThat(wire.proposal.receiveTimeNanos()).isEqualTo(101);
            assertThat(wire.proposal.completeTimeNanos()).isEqualTo(102);
            assertThat(wire.proposal.readyTimeNanos()).isEqualTo(103);
            definition(collector, collector.session(), 2, 2);
            observation(collector, 2, "two".getBytes(StandardCharsets.UTF_8), 2, 2);
        } finally { collector.onClose(); }
    }

    @Test void refusesUnattachedPollingAndInvalidRecoveryEnvelopes() throws IOException {
        final NodeFileCollector collector = new NodeFileCollector(write("one\n"), System.nanoTime());
        try {
            assertThatThrownBy(collector::doWork).isInstanceOf(IllegalStateException.class).hasMessageContaining("attach");
            assertThatThrownBy(() -> collector.attach(null, 1, 1)).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> collector.recovered(new UnsafeBuffer(new byte[8]), 0, 8, null))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("recovery");
        } finally { collector.onClose(); }
    }

    @Test void preservesUnsignedConfigurationAndClockIds() throws IOException {
        final NodeFileCollector collector = new NodeFileCollector(write("one\n"), System.nanoTime());
        try {
            definition(collector, collector.session(), -2, Integer.MIN_VALUE);
            observation(collector, 1, "one".getBytes(StandardCharsets.UTF_8), -2, Integer.MIN_VALUE);
        } finally { collector.onClose(); }
    }

    private Path write(final String lines) throws IOException {
        final Path source = directory.resolve("segment");
        Files.writeString(source, lines);
        return source;
    }
    private void definition(final NodeFileCollector collector, final String session, final int config, final int clock) {
        final byte[][] labels = Arrays.stream(new String[]{"node", "all", session, "file", "{}", "relative"})
            .map(value -> value.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final int length = codec.encodeDefinition(payload, 0, -1, labels);
        final UnsafeBuffer proposal = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + length]);
        wire.encodeProposal(proposal, 0, 42, 10, 1, 7, config, clock, -1, -1, -1, payload, 0, length);
        collector.recovered(proposal, 0, proposal.capacity(), null);
    }
    private void observation(final NodeFileCollector collector, final long sequence, final String raw) {
        observation(collector, sequence, raw.getBytes(StandardCharsets.UTF_8), 1, 1);
    }
    private void observation(final NodeFileCollector collector, final long sequence, final byte[] raw, final int config, final int clock) {
        final UnsafeBuffer proposal = proposal(sequence, raw, config, clock);
        collector.recovered(proposal, 0, proposal.capacity(), null);
    }
    private UnsafeBuffer proposal(final long sequence, final byte[] raw, final int config, final int clock) {
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        final int length = codec.encodeObservation(payload, 0, sequence, 0, CaptureCodec.UNKNOWN_NATIVE_ID,
            null, 0, 0, new UnsafeBuffer(raw), 0, raw.length);
        final UnsafeBuffer proposal = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + length]);
        wire.encodeProposal(proposal, 0, 42, 10, sequence, 7, config, clock, 101, 102, 103, payload, 0, length);
        return proposal;
    }
}
