package io.github.jstadeusiak.spine.sequencer;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.github.jstadeusiak.spine.feeds.CapturedIngress;
import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class CaptureRecordingIdentityTest {
    private static final long LOG = 42, PRODUCER = 10;
    private static final int SOURCE = 7, CAPTURE_STREAM = 7957, REPLAY_STREAM = 8957;
    @TempDir Path directory;

    @Test @Timeout(60)
    void captureWaitsForRegistrationThenPreservesExactAdmissionAndRecovery() throws Exception {
        try (ClusterFixture fixture = new ClusterFixture(directory, new SequencerService(), true);
             AeronArchive archive = fixture.connectArchive(9957)) {
            fixture.connect();
            final Wire wire = new Wire();
            final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
            fixture.exchange(init, wire.encodeInitialize(init, 0, LOG));
            fixture.expectClosedConsumerControlResponse(archive);

            final Aeron aeron = archive.context().aeron();
            final CountersReader nativeCounters = aeron.countersReader();
            final DelayedRegistration counters = new DelayedRegistration(nativeCounters);
            final Object conductor = field(Aeron.class, "conductor").get(aeron);
            final Field reader = field(conductor.getClass(), "countersReader");
            reader.set(conductor, counters);
            assertThat(aeron.countersReader()).isSameAs(counters);

            try {
                exerciseCapture(fixture, archive, counters);
                assertThat(counters.zeroReads).as("constructor waits through both unpublished registration reads").isEqualTo(2);
            } finally {
                reader.set(conductor, nativeCounters);
            }
            fixture.assertHealthy();
        }
    }

    private static void exerciseCapture(final ClusterFixture fixture, final AeronArchive archive,
        final DelayedRegistration counters) throws Exception {
        final UnsafeBuffer raw = new UnsafeBuffer(new byte[]{'{', '}'});
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[CaptureCodec.OBSERVATION_OVERHEAD + raw.capacity()]);
        final int payloadLength = new CaptureCodec().encodeObservation(payload, 0, 1, 0, 1,
            null, 0, 0, raw, 0, raw.capacity());
        final UnsafeBuffer exact = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payloadLength]);
        final Wire wire = new Wire();
        wire.encodeProposal(exact, 0, LOG, PRODUCER, 1, SOURCE, 11, 13, 100, 101, 102,
            payload, 0, payloadLength);
        final long recording;
        try (CapturedIngress ingress = new CapturedIngress(archive, fixture.producerContext(), LOG, PRODUCER,
            SOURCE, CAPTURE_STREAM, REPLAY_STREAM, null)) {
            assertThat(field(CapturedIngress.class, "registration").getLong(ingress)).isEqualTo(counters.registration);
            recording = field(CapturedIngress.class, "recordingId").getLong(ingress);
            assertThat(ingress.submit(11, 13, 100, 101, 102, payload, 0, payloadLength)).isTrue();
            fixture.await(ingress::idle, ingress::poll, "captured proposal admission");
            assertThat(ingress.acknowledgedSequence()).isEqualTo(1);
        }
        fixture.await(() -> archive.getStopPosition(recording) != AeronArchive.NULL_POSITION,
            () -> { }, "capture recording stopped");
        final int[] recovered = {0};
        try (CapturedIngress ingress = new CapturedIngress(archive, fixture.producerContext(), LOG, PRODUCER,
            SOURCE, CAPTURE_STREAM, REPLAY_STREAM, (bytes, offset, length, header) -> {
                assertThat(header).isNull();
                final byte[] actual = new byte[length];
                bytes.getBytes(offset, actual);
                assertThat(actual).isEqualTo(exact.byteArray());
                recovered[0]++;
            })) {
            assertThat(recovered[0]).isEqualTo(1);
            fixture.await(ingress::idle, ingress::poll, "exact captured retry admission");
            assertThat(ingress.acknowledgedSequence()).isEqualTo(1);
            final byte[] retry = fixture.exchange(exact, exact.capacity());
            assertThat(wire.validResult(new UnsafeBuffer(retry), 0, retry.length)).isTrue();
            assertThat(wire.result.status()).isEqualTo(AdmissionStatus.DUPLICATE);
            assertThat(wire.event.sequence()).isEqualTo(1);
        }
    }

    private static Field field(final Class<?> type, final String name) throws ReflectiveOperationException {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    /** Hide the first two registration reads for this capture; native buffers and transport remain live. */
    private static final class DelayedRegistration extends CountersReader {
        private int zeroReads;
        private long registration;

        DelayedRegistration(final CountersReader nativeCounters) {
            super(nativeCounters.metaDataBuffer(), nativeCounters.valuesBuffer());
        }

        @Override public long getCounterRegistrationId(final int counterId) {
            if (zeroReads < 2 && getCounterTypeId(counterId) == RecordingPos.RECORDING_POSITION_TYPE_ID
                && getCounterLabel(counterId).contains(" " + CAPTURE_STREAM + " aeron:ipc")) {
                final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while ((registration = super.getCounterRegistrationId(counterId)) == DEFAULT_REGISTRATION_ID) {
                    if (System.nanoTime() >= deadline) { throw new AssertionError("native capture registration was not published"); }
                    Thread.onSpinWait();
                }
                zeroReads++;
                return DEFAULT_REGISTRATION_ID;
            }
            return super.getCounterRegistrationId(counterId);
        }
    }
}
