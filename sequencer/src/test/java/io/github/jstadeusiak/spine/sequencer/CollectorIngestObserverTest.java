package io.github.jstadeusiak.spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.ImageFragmentAssembler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real IPC/Archive failure fixtures for diagnostics; reflection keeps the retained profile's private API unchanged. */
@Timeout(60)
class CollectorIngestObserverTest {
    private static final String CHANNEL = "aeron:ipc?term-length=1m|alias=spine-capture-10";
    @TempDir Path directory;

    @Test
    void maximumFragmentedObservationUsesCompletePhysicalPositionAndRecordedFence() throws Exception {
        try (Harness test = new Harness(directory)) {
            test.write(test.definition(10));
            test.pollUntil(1);
            assertThat(test.samples("capturePosition")[0]).isEqualTo(test.position);
            test.write(test.observation(2, "{\"result\":null,\"id\":1}".getBytes(StandardCharsets.UTF_8)));
            test.pollUntil(2);
            final byte[] maximum = new byte[Wire.MAX_PAYLOAD_BYTES - CaptureCodec.OBSERVATION_OVERHEAD];
            Arrays.fill(maximum, (byte)'x');
            ((byte[][])field(test.source, "raw"))[0] = maximum;
            final UnsafeBuffer proposal = test.observation(3, maximum);
            assertThat(proposal.capacity()).isEqualTo(Wire.MAX_PROPOSAL_BYTES);
            assertThat(proposal.capacity()).isGreaterThan(test.publication.maxPayloadLength());
            test.write(proposal);
            test.pollUntil(3);
            assertThat(test.samples("capturePosition")[2]).isEqualTo(test.position);
            assertThat(test.samples("captureObserved")[2]).isGreaterThanOrEqualTo(3);
            assertThat(test.samples("captureUpper")[2]).isGreaterThanOrEqualTo(3);
            assertThat(((ImageFragmentAssembler)field(test.stream, "assembler")).bufferBuilder().limit()).isZero();
        }
    }

    @Test
    void fullyRecordedIncompleteTailCreatesNoCompleteSampleAndNestedBeginFails() throws Exception {
        try (Harness test = new Harness(directory)) {
            final UnsafeBuffer bytes = test.definition(10);
            test.beginOnly(bytes);
            test.poll();
            assertThat(test.counters.getCounterValue(test.counter)).isGreaterThanOrEqualTo(test.position);
            assertThat((int)field(test.stream, "observed")).isZero();
            assertThat(test.samples("capturePosition")[0]).isEqualTo(-1);
            assertThat(test.samples("captureObserved")[0]).isEqualTo(-1);
            assertThat(test.samples("captureUpper")[0]).isEqualTo(-1);
            assertThat(((ImageFragmentAssembler)field(test.stream, "assembler")).bufferBuilder().limit()).isEqualTo(32);
            test.beginOnly(bytes);
            assertThatThrownBy(test::poll).isInstanceOf(IllegalStateException.class).hasMessage("observer callback failed")
                .hasRootCauseMessage("observer nested/discontinuous/orphaned fragment");
            assertThat((int)field(test.stream, "observed")).isZero();
        }
    }

    @Test
    void wrongProducerCallbackIsLatchedAndSurfacedOutsideImagePoll() throws Exception {
        try (Harness test = new Harness(directory)) {
            final UnsafeBuffer wrongProducer = test.definition(11);
            assertThat(new Wire().validProposal(wrongProducer, 0, wrongProducer.capacity())).isTrue();
            test.write(wrongProducer);
            assertThatThrownBy(test::poll).isInstanceOf(IllegalStateException.class).hasMessage("observer callback failed")
                .hasRootCauseMessage("observer identity/order differs");
            final Throwable failure = (Throwable)field(test.stream, "failure");
            assertThatThrownBy(test::poll).hasCause(failure);
            assertThat((int)field(test.stream, "observed")).isZero();
            assertThat(test.samples("capturePosition")[0]).isEqualTo(-1);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void stoppedRecordingOrMismatchedPinnedRegistrationFails(final boolean mismatch) throws Exception {
        try (Harness test = new Harness(directory)) {
            if (mismatch) {
                final long registration = (long)field(test.stream, "registration");
                set(test.stream, "registration", registration + 1);
                assertThat(RecordingPos.isActive(test.counters, test.counter, test.recording)).isTrue();
            } else {
                test.archive.stopRecording(test.publication);
                test.fixture.await(() -> !RecordingPos.isActive(test.counters, test.counter, test.recording),
                    () -> { }, "diagnostic fixture recorder stop");
            }
            assertThatThrownBy(test::poll).isInstanceOf(IllegalStateException.class)
                .hasMessage("observer recording stopped/counter reused");
            assertThat((int)field(test.stream, "observed")).isZero();
        }
    }

    private static final class Harness implements AutoCloseable {
        final ClusterFixture fixture;
        final AeronArchive archive;
        final ExclusivePublication publication;
        final CountersReader counters;
        final int counter;
        final long recording;
        final Object source, stream;
        long position;
        Harness(final Path directory) throws Exception {
            fixture = new ClusterFixture(directory, new SequencerService(), true);
            archive = fixture.connectArchive(9760);
            fixture.expectClosedConsumerControlResponse(archive);
            publication = archive.addRecordedExclusivePublication(CHANNEL, 8210);
            counters = archive.context().aeron().countersReader();
            final int[] found = {CountersReader.NULL_COUNTER_ID};
            fixture.await(() -> (found[0] = RecordingPos.findCounterIdBySession(counters,
                publication.sessionId(), archive.archiveId())) != CountersReader.NULL_COUNTER_ID, () -> { }, "observer fixture recorder");
            counter = found[0]; recording = RecordingPos.getRecordingId(counters, counter);
            final long origin = System.nanoTime();
            final Class<?> sourceType = nested("Source"), auditType = nested("Audit");
            source = create(sourceType, new Class<?>[]{boolean.class, long.class, long.class}, false, origin, 1L);
            final Object sbe = create(sourceType, new Class<?>[]{boolean.class, long.class, long.class}, true, origin, 1L);
            final Object audit = create(auditType, new Class<?>[]{long.class, boolean.class, sourceType, sourceType}, origin, true, source, sbe);
            final byte[][] labels = new byte[6][];
            for (int i = 0; i < labels.length; i++) {
                labels[i] = new byte[CaptureCodec.MAX_LABEL_BYTES]; Arrays.fill(labels[i], (byte)('a' + i));
            }
            final UnsafeBuffer definition = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
            final int length = new CaptureCodec().encodeDefinition(definition, 0, -1, labels);
            final byte[] bytes = new byte[length]; definition.getBytes(0, bytes);
            set(source, "definition", bytes); set(source, "definitionReady", 0L);
            stream = create(nested("Stream"), new Class<?>[]{AeronArchive.class, ClusterFixture.class, auditType, sourceType,
                int.class, String.class}, archive, fixture, audit, source, 8210, "spine-capture-10");
        }
        UnsafeBuffer definition(final long producer) throws Exception {
            final byte[] bytes = (byte[])field(source, "definition");
            return proposal(producer, 1, -1, -1, 0, new UnsafeBuffer(bytes), bytes.length);
        }
        UnsafeBuffer observation(final long sequence, final byte[] raw) {
            final UnsafeBuffer payload = new UnsafeBuffer(new byte[CaptureCodec.OBSERVATION_OVERHEAD + raw.length]);
            final int size = new CaptureCodec().encodeObservation(payload, 0, sequence - 1, 0, CaptureCodec.UNKNOWN_NATIVE_ID,
                null, 0, 0, new UnsafeBuffer(raw), 0, raw.length);
            return proposal(10, sequence, 1, 2, 3, payload, size);
        }
        UnsafeBuffer proposal(final long producer, final long sequence, final long receive, final long complete,
            final long ready, final UnsafeBuffer payload, final int size) {
            final UnsafeBuffer bytes = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + size]);
            new Wire().encodeProposal(bytes, 0, 42, producer, sequence, 7, 1, 1, receive, complete, ready, payload, 0, size);
            return bytes;
        }
        void write(final UnsafeBuffer bytes) {
            fixture.await(() -> (position = publication.offer(bytes, 0, bytes.capacity())) > 0, () -> { }, "diagnostic fixture offer");
            recorded();
        }
        void beginOnly(final UnsafeBuffer bytes) {
            final BufferClaim claim = new BufferClaim();
            fixture.await(() -> (position = publication.tryClaim(32, claim)) > 0, () -> { }, "diagnostic fixture partial claim");
            claim.putBytes(bytes, 0, 32).flags(FrameDescriptor.BEGIN_FRAG_FLAG).commit(); recorded();
        }
        void recorded() {
            fixture.await(() -> counters.getCounterValue(counter) >= position, () -> { }, "diagnostic fixture complete recording");
        }
        void pollUntil(final int commands) {
            fixture.await(() -> {
                try { return (int)field(stream, "fenced") == commands; } catch (final Exception failure) { throw new AssertionError(failure); }
            }, this::pollUnchecked, "diagnostic complete-message/fence observation");
        }
        void pollUnchecked() { try { poll(); } catch (final Exception failure) { throw new AssertionError(failure); } }
        void poll() throws Exception { invoke(stream, "poll"); }
        long[] samples(final String name) throws Exception { return (long[])field(field(source, "boundaries"), name); }
        @Override public void close() throws Exception {
            invoke(stream, "close"); publication.close();
            fixture.await(() -> archive.getStopPosition(recording) != AeronArchive.NULL_POSITION, () -> { }, "diagnostic fixture stopped");
            archive.close(); fixture.assertHealthy(); fixture.abort(); fixture.close();
        }
    }
    private static Class<?> nested(final String name) throws ClassNotFoundException {
        return Class.forName(CollectorIngestProfile.class.getName() + "$" + name);
    }
    private static Object create(final Class<?> type, final Class<?>[] signature, final Object... arguments) throws Exception {
        final Constructor<?> constructor = type.getDeclaredConstructor(signature); constructor.setAccessible(true);
        try { return constructor.newInstance(arguments); } catch (final InvocationTargetException failure) { throwCause(failure); return null; }
    }
    private static Object field(final Object object, final String name) throws Exception {
        final Field field = object.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(object);
    }
    private static void set(final Object object, final String name, final Object value) throws Exception {
        final Field field = object.getClass().getDeclaredField(name); field.setAccessible(true); field.set(object, value);
    }
    private static void invoke(final Object object, final String name) throws Exception {
        final Method method = object.getClass().getDeclaredMethod(name); method.setAccessible(true);
        try { method.invoke(object); } catch (final InvocationTargetException failure) { throwCause(failure); }
    }
    private static void throwCause(final InvocationTargetException failure) throws Exception {
        if (failure.getCause() instanceof Exception exception) { throw exception; }
        if (failure.getCause() instanceof Error error) { throw error; }
        throw new AssertionError(failure.getCause());
    }
}
