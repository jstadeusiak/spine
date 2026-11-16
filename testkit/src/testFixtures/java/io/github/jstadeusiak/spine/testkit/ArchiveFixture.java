package io.github.jstadeusiak.spine.testkit;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.AeronException;
import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/** An isolated, synthetic, single-recording fixture. This is not a committed sequencer. */
public final class ArchiveFixture implements AutoCloseable {
    public static final String LIVE_CHANNEL = "aeron:ipc?term-length=64k|tether=false";
    public static final int LIVE_STREAM_ID = 1001;
    public static final String REPLAY_CHANNEL = "aeron:ipc?term-length=64k";
    private static final int CONTROL_STREAM_ID = 101;

    private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
    private final List<Throwable> expectedShutdownWarnings = new ArrayList<>();
    private Predicate<Throwable> expectedWarning = error -> false;
    private final ArchivingMediaDriver driver;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final Publication publication;
    private final long logId = 42;
    private final Wire wire = new Wire();
    private final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(8192);
    private final long recordingId;
    private final int recordingCounterId;
    private int responseStreamId = 201;

    public ArchiveFixture(final Path directory) {
        final String aeronDirectory = directory.resolve("driver").toString();
        driver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectory)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.SHARED)
                .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(5))
                .ipcTermBufferLength(64 * 1024)
                .ipcMtuLength(1024)
                .untetheredWindowLimitTimeoutNs(TimeUnit.MILLISECONDS.toNanos(20))
                .untetheredLingerTimeoutNs(TimeUnit.MILLISECONDS.toNanos(20))
                .untetheredRestingTimeoutNs(TimeUnit.MILLISECONDS.toNanos(50))
                .errorHandler(errors::add),
            new Archive.Context()
                .archiveDir(directory.resolve("archive").toFile())
                .deleteArchiveOnStart(true)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .controlChannelEnabled(false)
                .localControlChannel("aeron:ipc?term-length=64k")
                .localControlStreamId(CONTROL_STREAM_ID)
                .archiveClientContext(new AeronArchive.Context()
                    .controlRequestChannel("aeron:ipc?term-length=64k")
                    .controlRequestStreamId(CONTROL_STREAM_ID)
                    .controlResponseChannel("aeron:ipc?term-length=64k")
                    .controlResponseStreamId(199))
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .fileSyncLevel(0)
                .errorHandler(errors::add));
        Aeron openedAeron = null;
        AeronArchive openedArchive = null;
        Publication openedPublication = null;
        try {
            aeron = openedAeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(aeronDirectory).errorHandler(errors::add));
            archive = openedArchive = connectArchive();
            publication = openedPublication = archive.addRecordedPublication("aeron:ipc?term-length=64k|alias=spine-output", LIVE_STREAM_ID);
            final CountersReader counters = aeron.countersReader();
            await(() -> RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId())
                != CountersReader.NULL_COUNTER_ID, () -> { });
            recordingCounterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId(), archive.archiveId());
            recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);
        } catch (final RuntimeException | Error failure) {
            CloseHelper.quietCloseAll(openedPublication, openedArchive, openedAeron, driver);
            throw failure;
        }
    }

    /** Each caller gets an independent control response stream, sharing only the borrowed Aeron client. */
    public AeronArchive connectArchive() {
        return AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel("aeron:ipc?term-length=64k")
            .controlRequestStreamId(CONTROL_STREAM_ID)
            .controlResponseChannel("aeron:ipc?term-length=64k")
            .controlResponseStreamId(responseStreamId++)
            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10)));
    }

    public long logId() { return logId; }
    public long recordingId() { return recordingId; }

    /** Records a synthetic event. Sequence assignment here exists only for recovery tests. */
    public void publish(final long sequence, final int payloadBytes, final Runnable pump) {
        final var payload = ExamplePayload.encode(sequence, payloadBytes);
        final int length = wire.encodeEvent(frame, 0, logId, sequence, sequence * 10 + 3, 1, 1, 1,
            sequence * 10, sequence * 10 + 1, sequence * 10 + 2, payload, 0, payload.capacity());
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (publication.offer(frame, 0, length) <= 0) {
            pump.run();
            assertHealthy();
            if (publication.isClosed() || System.nanoTime() >= deadline) {
                throw new IllegalStateException("recorded publication made no progress");
            }
            LockSupport.parkNanos(100_000);
        }
        pump.run();
    }

    public void awaitRecorded(final Runnable pump) {
        final long position = publication.position();
        await(() -> aeron.countersReader().getCounterValue(recordingCounterId) >= position, pump);
    }

    public void await(final BooleanSupplier condition, final Runnable pump) {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.getAsBoolean()) {
            pump.run();
            assertHealthy();
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("fixture condition timed out");
            }
            LockSupport.parkNanos(100_000);
        }
    }

    /** Aeron can acknowledge stop-replay after the consumer has closed its private response subscription. */
    public void expectClosedConsumerControlResponse(final AeronArchive owner) {
        final String ownerSession = ChannelUri.parse(owner.context().controlResponseChannel()).get("session-id");
        final int streamId = owner.context().controlResponseStreamId();
        expectedWarning = expectedWarning.or(error -> error instanceof ArchiveEvent event
            && event.category() == AeronException.Category.WARN
            && event.getMessage().startsWith("WARN - control response publication is not connected: ControlSession{")
            && event.getMessage().contains("state=DONE,")
            && event.getMessage().contains("streamId=" + streamId + ",")
            && !event.getMessage().contains("sessionId=" + ownerSession + ","));
    }

    public void assertHealthy() {
        for (Throwable error; (error = errors.peek()) != null; ) {
            if (expectedWarning.test(error)) {
                expectedShutdownWarnings.add(errors.remove());
                continue;
            }
            throw new IllegalStateException("embedded Aeron/Archive failure", error);
        }
    }

    @Override
    public void close() {
        CloseHelper.closeAll(publication, archive, aeron, driver);
    }
}
