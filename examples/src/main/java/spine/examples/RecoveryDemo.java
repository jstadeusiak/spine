package spine.examples;

import io.aeron.archive.client.AeronArchive;
import spine.client.CatalogSubscription;
import spine.client.EventHandler;
import spine.testkit.ArchiveFixture;
import spine.testkit.CountingProjection;
import org.agrona.IoUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Two independent consumers of a synthetic Archive recording. The delayed consumer resumes from a checkpoint. */
public final class RecoveryDemo {
    public static void main(final String[] args) throws IOException {
        int events = 400;
        int payloadBytes = 4096;
        for (int i = 0; i < args.length; i += 2) {
            final String option = args[i];
            if (i + 1 == args.length) {
                throw new IllegalArgumentException("missing value for " + option);
            }
            switch (option) {
                case "--events" -> events = Integer.parseInt(args[i + 1]);
                case "--payload-bytes" -> payloadBytes = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException("unknown option " + option);
            }
        }
        if (events < 1) {
            throw new IllegalArgumentException("--events must be positive");
        }
        demonstrateRecovery(events, payloadBytes);
    }

    private static void demonstrateRecovery(final int events, final int payloadBytes) throws IOException {
        final Path directory = Files.createTempDirectory("spine-recovery-");
        try (ArchiveFixture fixture = new ArchiveFixture(directory);
             AeronArchive eagerArchive = fixture.connectArchive();
             AeronArchive delayedArchive = fixture.connectArchive()) {
            fixture.expectClosedConsumerControlResponse(eagerArchive);
            fixture.expectClosedConsumerControlResponse(delayedArchive);
            final CountingProjection eager = new CountingProjection();
            final CountingProjection audit = new CountingProjection();
            final CountingProjection delayed = new CountingProjection();
            final EventHandler eagerHandler = eager::apply;
            final EventHandler groupedHandler = eagerHandler.andThen(audit::apply);
            final EventHandler delayedHandler = delayed::apply;
            final byte[] checkpoint;
            final long checkpointSequence;

            try (CatalogSubscription eagerReader = reader(eagerArchive, fixture, 0, 4001);
                 CatalogSubscription delayedReader = reader(delayedArchive, fixture, 0, 4002)) {
                final Runnable pollEager = () -> eagerReader.poll(groupedHandler, 256);
                final Runnable pollBoth = () -> {
                    pollEager.run();
                    delayedReader.poll(delayedHandler, 256);
                };
                final int pauseAfter = Math.min(20, events - 1);
                for (long sequence = 1; sequence <= pauseAfter; sequence++) {
                    fixture.publish(sequence, payloadBytes, pollBoth);
                }
                fixture.await(() -> delayed.count() == pauseAfter && eager.count() == pauseAfter, pollBoth);
                checkpoint = delayed.snapshot();
                checkpointSequence = delayedReader.sequence();

                for (long sequence = pauseAfter + 1; sequence <= events; sequence++) {
                    fixture.publish(sequence, payloadBytes, pollEager);
                }
                fixture.awaitRecorded(pollEager);
                fixture.await(() -> delayed.count() == events && eager.count() == events, pollBoth);
                requireSame(eager, audit);
                requireSame(eager, delayed);
                System.out.printf("PASS composed projections matched; paused Archive follower caught up through %d%n",
                    delayedReader.sequence());
            }

            final CountingProjection restored = new CountingProjection();
            restored.restore(checkpoint);
            final EventHandler restoredHandler = restored::apply;
            try (CatalogSubscription resumed = reader(delayedArchive, fixture, checkpointSequence, 4003)) {
                fixture.await(() -> restored.count() == events, () -> resumed.poll(restoredHandler, 256));
                requireSame(eager, restored);
                System.out.printf("PASS application checkpoint replayed from %d through %d%n",
                    checkpointSequence, resumed.sequence());
            }
        } finally {
            IoUtil.delete(directory.toFile(), false);
        }
    }

    private static CatalogSubscription reader(final AeronArchive archive, final ArchiveFixture fixture,
        final long sequence, final int stream) {
        return new CatalogSubscription(archive, fixture.logId(), ArchiveFixture.LIVE_STREAM_ID, sequence,
            ArchiveFixture.REPLAY_CHANNEL, stream);
    }

    private static void requireSame(final CountingProjection expected, final CountingProjection actual) {
        if (!Arrays.equals(expected.snapshot(), actual.snapshot())) {
            throw new IllegalStateException("projection state differs");
        }
    }
}
