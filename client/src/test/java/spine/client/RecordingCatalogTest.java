package spine.client;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import spine.protocol.Wire;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordingCatalogTest {
    @TempDir Path directory;

    @Test void repeatedMemberAndReplayOutputIsVerifiedAndAppliedOnceAcrossOverlappingSpans() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation first = fixture.generation();
            for (int sequence = 1; sequence <= 3; sequence++) { fixture.publish(first, 7, sequence, 8); }
            fixture.stop(first);
            final Generation replayed = fixture.generation();
            for (int sequence = 2; sequence <= 5; sequence++) { fixture.publish(replayed, 7, sequence, 8); }
            fixture.recorded(replayed);
            assertThat(fixture.catalog().headSequence()).isEqualTo(5);
            try (CatalogSubscription consumer = fixture.consumer(0)) {
                final List<Long> applied = new ArrayList<>();
                final EventHandler handler = (buffer, offset, length, event) -> applied.add(event.sequence());
                Fixture.await(() -> consumer.sequence() == 5, () -> consumer.poll(handler, 10));
                assertThat(applied).containsExactly(1L, 2L, 3L, 4L, 5L);
                fixture.publish(replayed, 7, 6, Wire.MAX_PAYLOAD_BYTES);
                Fixture.await(() -> consumer.sequence() == 6, () -> consumer.poll(handler, 10));
                assertThat(applied).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
            }
        }
    }

    @Test void conflictingOverlapForeignLogAndMissingPrefixFailQualification() {
        try (Fixture fixture = new Fixture(directory.resolve("conflict"))) {
            final Generation first = fixture.generation();
            fixture.publish(first, 7, 1, 8);
            fixture.stop(first);
            final Generation second = fixture.generation();
            fixture.payload.putByte(7, (byte)1);
            fixture.publish(second, 7, 1, 8);
            fixture.recorded(second);
            assertThatThrownBy(fixture::catalog).hasMessageContaining("conflicting");
        }
        try (Fixture fixture = new Fixture(directory.resolve("foreign"))) {
            final Generation generation = fixture.generation();
            fixture.publish(generation, 8, 1, 8);
            fixture.recorded(generation);
            assertThatThrownBy(fixture::catalog).hasMessageContaining("foreign log");
        }
        try (Fixture fixture = new Fixture(directory.resolve("gap"))) {
            final Generation generation = fixture.generation();
            fixture.publish(generation, 7, 2, 8);
            fixture.recorded(generation);
            assertThatThrownBy(fixture::catalog).hasMessageContaining("gap");
        }
    }

    @Test void multipleActiveMembersAndIndependentCheckpointCatchupAreSupported() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation first = fixture.generation();
            final Generation second = fixture.generation();
            for (int sequence = 1; sequence <= 5; sequence++) {
                fixture.publish(first, 7, sequence, 8);
                fixture.publish(second, 7, sequence, 8);
            }
            fixture.recorded(first);
            fixture.recorded(second);
            assertThat(fixture.catalog().spans()).hasSize(2);
            try (CatalogSubscription consumer = fixture.consumer(3)) {
                final List<Long> applied = new ArrayList<>();
                Fixture.await(() -> consumer.sequence() == 5,
                    () -> consumer.poll((buffer, offset, length, event) -> applied.add(event.sequence()), 10));
                assertThat(applied).containsExactly(4L, 5L);
            }
        }
    }

    @Test void trustedCheckpointRecoversAfterActualSegmentPurgeAndStalePhysicalReplayFailsClosed() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation generation = fixture.generation();
            int firstRetained = 0;
            for (int sequence = 1; sequence <= 40; sequence++) {
                final long before = generation.publication().position();
                fixture.publish(generation, 7, sequence, Wire.MAX_PAYLOAD_BYTES);
                if (before <= Fixture.SEGMENT && generation.publication().position() > Fixture.SEGMENT) {
                    firstRetained = sequence;
                }
            }
            fixture.stop(generation);
            final RecordingCatalog.Span stale = fixture.catalog().select(1);
            assertThat(firstRetained).isGreaterThan(1);
            assertThat(fixture.archive.purgeSegments(generation.recordingId(), Fixture.SEGMENT)).isPositive();
            assertThat(fixture.archive.getStartPosition(generation.recordingId())).isEqualTo(Fixture.SEGMENT);
            assertThatThrownBy(() -> new BoundedReplay(fixture.archive, stale.recordingId(), stale.startPosition(),
                stale.completePosition(), Fixture.CHANNEL, 1140, false))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("unavailable history").hasMessageContaining("purged");
            assertThatThrownBy(fixture::catalog).hasMessageContaining("unavailable history").hasMessageContaining("gap");
            final int checkpoint = firstRetained - 1;
            assertThatThrownBy(() -> fixture.catalog(checkpoint - 1)).hasMessageContaining("unavailable history");
            try (CatalogSubscription consumer = fixture.consumer(checkpoint)) {
                final List<Long> applied = new ArrayList<>();
                Fixture.await(() -> consumer.sequence() == 40,
                    () -> consumer.poll((buffer, offset, length, event) -> applied.add(event.sequence()), 100));
                assertThat(applied).hasSize(40 - checkpoint);
                for (int i = 0; i < applied.size(); i++) { assertThat(applied.get(i)).isEqualTo(checkpoint + i + 1L); }
            }
        }
    }

    @Test void checkpointRequiresRecordedHeadEvidenceAndCompleteUnconflictedSuffix() {
        try (Fixture fixture = new Fixture(directory.resolve("empty"))) {
            assertThatThrownBy(() -> fixture.consumer(1)).hasMessageContaining("unavailable history")
                .hasMessageContaining("beyond recorded head");
        }
        try (Fixture fixture = new Fixture(directory.resolve("suffix"))) {
            final Generation first = fixture.generation();
            for (int sequence = 5; sequence <= 7; sequence++) { fixture.publish(first, 7, sequence, 8); }
            fixture.stop(first);
            final Generation second = fixture.generation();
            for (int sequence = 6; sequence <= 9; sequence++) { fixture.publish(second, 7, sequence, 8); }
            fixture.recorded(second);
            assertThat(fixture.catalog(4).headSequence()).isEqualTo(9);
            assertThatThrownBy(() -> fixture.catalog(10)).hasMessageContaining("unavailable history");
            final Generation conflicting = fixture.generation();
            fixture.payload.putByte(7, (byte)1);
            fixture.publish(conflicting, 7, 6, 8);
            fixture.recorded(conflicting);
            assertThatThrownBy(() -> fixture.catalog(4)).hasMessageContaining("conflicting");
        }
        try (Fixture fixture = new Fixture(directory.resolve("gap"))) {
            final Generation generation = fixture.generation();
            fixture.publish(generation, 7, 5, 8);
            fixture.publish(generation, 7, 7, 8);
            fixture.recorded(generation);
            assertThatThrownBy(() -> fixture.catalog(4)).hasMessageContaining("noncontiguous");
        }
    }

    @Test void checkpointedPrefixIsNotComparedOrAppliedAgain() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation first = fixture.generation();
            for (int sequence = 1; sequence <= 5; sequence++) { fixture.publish(first, 7, sequence, 8); }
            fixture.stop(first);
            final Generation second = fixture.generation();
            fixture.payload.putByte(7, (byte)1);
            fixture.publish(second, 7, 3, 8);
            fixture.payload.putByte(7, (byte)0);
            fixture.publish(second, 7, 4, 8);
            fixture.publish(second, 7, 5, 8);
            fixture.recorded(second);
            assertThatThrownBy(fixture::catalog).hasMessageContaining("conflicting");
            try (CatalogSubscription consumer = fixture.consumer(3)) {
                final List<Long> applied = new ArrayList<>();
                Fixture.await(() -> consumer.sequence() == 5,
                    () -> consumer.poll((buffer, offset, length, event) -> applied.add(event.sequence()), 10));
                assertThat(applied).containsExactly(4L, 5L);
            }
        }
    }

    @Test void separateDriverConsumerUsesArchiveProtocolAuthorityAndUdpReplay() throws Exception {
        // Bind the publisher as well as its destination to loopback on multi-interface hosts.
        final String control = "aeron:udp?endpoint=127.0.0.1:" + availablePort() + "|interface=127.0.0.1";
        try (Fixture fixture = new Fixture(directory.resolve("server"), control);
             MediaDriver remoteDriver = MediaDriver.launch(new MediaDriver.Context()
                 .aeronDirectoryName(directory.resolve("consumer-driver").toString())
                 .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED));
             Aeron remoteAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(remoteDriver.aeronDirectoryName()));
             AeronArchive remoteArchive = AeronArchive.connect(new AeronArchive.Context().aeron(remoteAeron).ownsAeronClient(false)
                 .controlRequestChannel(control).controlRequestStreamId(101)
                 .controlResponseChannel("aeron:udp?endpoint=127.0.0.1:0|interface=127.0.0.1").controlResponseStreamId(201)
                 .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)))) {
            final Generation generation = fixture.generation();
            fixture.publish(generation, 7, 1, 8);
            fixture.publish(generation, 7, 2, Wire.MAX_PAYLOAD_BYTES);
            fixture.recorded(generation);
            assertThat(RecordingPos.findCounterIdBySession(remoteAeron.countersReader(), generation.publication().sessionId(),
                fixture.archive.archiveId())).isEqualTo(CountersReader.NULL_COUNTER_ID);
            final String replay = "aeron:udp?endpoint=127.0.0.1:" + availablePort() + "|interface=127.0.0.1";
            try (CatalogSubscription consumer = new CatalogSubscription(remoteArchive, 7, 1001, 1, replay, 1171)) {
                final List<Long> applied = new ArrayList<>();
                final EventHandler handler = (buffer, offset, length, event) -> applied.add(event.sequence());
                Fixture.await(() -> consumer.sequence() == 2, () -> consumer.poll(handler, 100));
                fixture.publish(generation, 7, 3, Wire.MAX_PAYLOAD_BYTES);
                Fixture.await(() -> consumer.sequence() == 3, () -> consumer.poll(handler, 100));
                fixture.stop(generation);
                Fixture.await(consumer::requiresRefresh, () -> consumer.poll(handler, 100));
                final Generation next = fixture.generation();
                fixture.publish(next, 7, 4, 8);
                fixture.recorded(next);
                consumer.refresh();
                Fixture.await(() -> consumer.sequence() == 4, () -> consumer.poll(handler, 100));
                assertThat(applied).containsExactly(2L, 3L, 4L);
            }
        }
    }

    private static int availablePort() throws Exception {
        try (DatagramSocket socket = new DatagramSocket(0, InetAddress.getLoopbackAddress())) { return socket.getLocalPort(); }
    }

    @Test void applicationFailureStopsWithoutAdvancingAndBorrowedArchiveRemainsOpen() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation generation = fixture.generation();
            fixture.publish(generation, 7, 1, 8);
            fixture.recorded(generation);
            try (CatalogSubscription consumer = fixture.consumer(0)) {
                assertThatThrownBy(() -> Fixture.await(() -> false,
                    () -> consumer.poll((buffer, offset, length, event) -> { throw new IllegalArgumentException("app failed"); }, 10)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessage("app failed");
                assertThat(consumer.sequence()).isZero();
                assertThat(consumer.status()).isEqualTo(SubscriptionStatus.FAILED);
                assertThat(fixture.archive.getRecordingPosition(generation.recordingId())).isPositive();
            }
        }
    }

    @Test void stoppedGenerationIsExplicitlyRequalifiedWithoutIdleRescans() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation first = fixture.generation();
            fixture.publish(first, 7, 1, 8);
            fixture.recorded(first);
            try (CatalogSubscription consumer = fixture.consumer(0)) {
                final List<Long> applied = new ArrayList<>();
                final EventHandler handler = (buffer, offset, length, event) -> applied.add(event.sequence());
                Fixture.await(() -> consumer.sequence() == 1, () -> consumer.poll(handler, 10));
                fixture.stop(first);
                Fixture.await(consumer::requiresRefresh, () -> consumer.poll(handler, 10));
                final Generation second = fixture.generation();
                fixture.publish(second, 7, 1, 8);
                fixture.publish(second, 7, 2, 8);
                fixture.recorded(second);
                consumer.refresh();
                Fixture.await(() -> consumer.sequence() == 2, () -> consumer.poll(handler, 10));
                assertThat(applied).containsExactly(1L, 2L);
            }
        }
    }

    @Test void anUnqualifiedEmptyRecoverySourceCannotAssertItsStartingSequence() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation first = fixture.generation();
            fixture.publish(first, 7, 1, 8);
            fixture.stop(first);
            final Generation recovering = fixture.generation();
            assertThat(fixture.catalog().select(2)).isNull();
            fixture.publish(recovering, 7, 1, 8);
            fixture.recorded(recovering);
            assertThat(fixture.catalog().select(2).recordingId()).isEqualTo(recovering.recordingId());
        }
    }

    @Test void malformedFutureFrameLatchesFailureInsteadOfBeingSwallowedByImagePoll() {
        try (Fixture fixture = new Fixture(directory)) {
            final Generation generation = fixture.generation();
            fixture.publish(generation, 7, 1, 8);
            fixture.recorded(generation);
            try (CatalogSubscription consumer = fixture.consumer(0)) {
                final EventHandler handler = (buffer, offset, length, event) -> { };
                Fixture.await(() -> consumer.sequence() == 1, () -> consumer.poll(handler, 10));
                final int length = fixture.wire.encodeEvent(fixture.frame, 0, 7, 2, 200, 1, 1, 1,
                    20, 21, 22, fixture.payload, 0, 8);
                fixture.frame.putShort(2, (short)999, Wire.ORDER);
                Fixture.await(() -> generation.publication().offer(fixture.frame, 0, length) > 0, () -> { });
                assertThatThrownBy(() -> Fixture.await(() -> false, () -> consumer.poll(handler, 10)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("malformed");
                assertThat(consumer.sequence()).isEqualTo(1);
                assertThat(consumer.status()).isEqualTo(SubscriptionStatus.FAILED);
            }
        }
    }

    @Test void actualArchiveReplayPollingWithMaximumFragmentedMessagesDoesNotAllocatePerEvent() {
        final com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean)
            java.lang.management.ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        final long thread = Thread.currentThread().threadId();
        assertThat(bean.getThreadAllocatedBytes(thread)).isNotNegative();
        try (Fixture fixture = new Fixture(directory)) {
            final Generation generation = fixture.generation();
            try (CatalogSubscription consumer = fixture.consumer(0)) {
                final EventHandler handler = (buffer, offset, length, event) -> { };
                for (int sequence = 1; sequence <= 1000; sequence++) {
                    fixture.publish(generation, 7, sequence, Wire.MAX_PAYLOAD_BYTES);
                    final long expected = sequence;
                    Fixture.await(() -> consumer.sequence() == expected, () -> consumer.poll(handler, 100));
                }
                long allocated = 0;
                for (int sequence = 1001; sequence <= 1500; sequence++) {
                    fixture.publish(generation, 7, sequence, Wire.MAX_PAYLOAD_BYTES);
                    final long start = bean.getThreadAllocatedBytes(thread);
                    while (consumer.sequence() < sequence) { consumer.poll(handler, 100); }
                    allocated += bean.getThreadAllocatedBytes(thread) - start;
                }
                assertThat(allocated).isLessThanOrEqualTo(4096);
                assertThat(consumer.sequence()).isEqualTo(1500);
            }
        }
    }

    private record Generation(ExclusivePublication publication, long recordingId, int counterId) { }

    private static final class Fixture implements AutoCloseable {
        private static final String CHANNEL = "aeron:ipc?term-length=1m";
        private static final int SEGMENT = 1024 * 1024;
        private final ArchivingMediaDriver driver;
        private final Aeron aeron;
        private final AeronArchive archive;
        private final Wire wire = new Wire();
        private final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
        private final UnsafeBuffer frame = new UnsafeBuffer(new byte[Wire.MAX_EVENT_BYTES]);
        private final List<ExclusivePublication> publications = new ArrayList<>();

        Fixture(final Path directory) { this(directory, null); }

        Fixture(final Path directory, final String remoteControl) {
            final String aeronDirectory = directory.resolve("driver").toString();
            driver = ArchivingMediaDriver.launch(new MediaDriver.Context().aeronDirectoryName(aeronDirectory)
                .dirDeleteOnStart(true).dirDeleteOnShutdown(true).threadingMode(ThreadingMode.SHARED)
                .ipcTermBufferLength(1024 * 1024).ipcMtuLength(1024),
                new Archive.Context().archiveDir(directory.resolve("archive").toFile()).deleteArchiveOnStart(true)
                    .threadingMode(ArchiveThreadingMode.SHARED).controlChannelEnabled(remoteControl != null)
                    .controlChannel(remoteControl == null ? "aeron:udp?endpoint=127.0.0.1:0" : remoteControl).controlStreamId(101)
                    .segmentFileLength(SEGMENT)
                    .localControlChannel(CHANNEL).localControlStreamId(101)
                    .archiveClientContext(new AeronArchive.Context().controlRequestChannel(CHANNEL)
                        .controlRequestStreamId(101).controlResponseChannel(CHANNEL).controlResponseStreamId(199))
                    .replicationChannel("aeron:udp?endpoint=localhost:0").fileSyncLevel(0));
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
            archive = AeronArchive.connect(new AeronArchive.Context().aeron(aeron).ownsAeronClient(false)
                .controlRequestChannel(CHANNEL).controlRequestStreamId(101)
                .controlResponseChannel(CHANNEL).controlResponseStreamId(201)
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
        }

        Generation generation() {
            final ExclusivePublication publication = archive.addRecordedExclusivePublication(CHANNEL + "|alias=spine-output", 1001);
            publications.add(publication);
            final CountersReader counters = aeron.countersReader();
            await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId())
                != CountersReader.NULL_COUNTER_ID, () -> { });
            final int counter = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
            return new Generation(publication, RecordingPos.getRecordingId(counters, counter), counter);
        }

        void publish(final Generation generation, final long logId, final long sequence, final int payloadLength) {
            final int length = wire.encodeEvent(frame, 0, logId, sequence, sequence * 100, 1, 1, 1,
                sequence * 10, sequence * 10 + 1, sequence * 10 + 2, payload, 0, payloadLength);
            await(() -> generation.publication().offer(frame, 0, length) > 0, () -> { });
        }

        void recorded(final Generation generation) {
            await(() -> aeron.countersReader().getCounterValue(generation.counterId()) >= generation.publication().position(), () -> { });
        }

        void stop(final Generation generation) {
            recorded(generation);
            generation.publication().close();
            await(() -> archive.getStopPosition(generation.recordingId()) != AeronArchive.NULL_POSITION, () -> { });
        }

        RecordingCatalog catalog() { return RecordingCatalog.rebuild(archive, 7, 1001, CHANNEL, 1101); }
        RecordingCatalog catalog(final long sequence) { return RecordingCatalog.rebuild(archive, 7, 1001, sequence, CHANNEL, 1101); }
        CatalogSubscription consumer(final long sequence) { return new CatalogSubscription(archive, 7, 1001, sequence, CHANNEL, 1102); }

        static void await(final BooleanSupplier condition, final Runnable pump) {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!condition.getAsBoolean()) {
                pump.run();
                if (System.nanoTime() >= deadline) { throw new IllegalStateException("fixture timed out"); }
                LockSupport.parkNanos(100_000);
            }
        }

        @Override public void close() {
            publications.forEach(ExclusivePublication::close);
            CloseHelper.closeAll(archive, aeron, driver);
        }
    }
}
