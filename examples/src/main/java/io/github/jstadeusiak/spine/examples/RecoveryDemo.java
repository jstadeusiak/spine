package io.github.jstadeusiak.spine.examples;

import io.aeron.archive.client.AeronArchive;
import io.github.jstadeusiak.spine.client.CatalogSubscription;
import io.github.jstadeusiak.spine.client.EventHandler;
import io.github.jstadeusiak.spine.testkit.ArchiveFixture;
import io.github.jstadeusiak.spine.testkit.CountingProjection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Independent followers of one synthetic Archive recording; the sequencer tests exercise actual Cluster input. */
public final class RecoveryDemo {
    public static void main(String[] args) throws Exception {
        int events = 400, payload = 4096;
        for (int i = 0; i < args.length; i += 2) {
            switch (args[i]) {
                case "--events" -> events = Integer.parseInt(args[i + 1]);
                case "--payload-bytes" -> payload = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException("unknown option");
            }
        }
        final int total = events;
        Path directory = Files.createTempDirectory("spine-recovery-");
        try (ArchiveFixture fixture = new ArchiveFixture(directory);
             AeronArchive a = fixture.connectArchive(); AeronArchive b = fixture.connectArchive()) {
            fixture.expectClosedConsumerControlResponse(a); fixture.expectClosedConsumerControlResponse(b);
            CountingProjection fast = new CountingProjection(), audit = new CountingProjection(), slow = new CountingProjection();
            EventHandler fastHandler = fast::apply, auditHandler = audit::apply, slowHandler = slow::apply;
            EventHandler eagerHandler = fastHandler.andThen(auditHandler);
            byte[] checkpoint;
            try (CatalogSubscription eager = reader(a, fixture, 0, 4001);
                 CatalogSubscription delayed = reader(b, fixture, 0, 4002)) {
                Runnable eagerPoll = () -> eager.poll(eagerHandler, 256);
                Runnable both = () -> { eagerPoll.run(); delayed.poll(slowHandler, 256); };
                for (int i = 1; i <= 20; i++) fixture.publish(i, payload, both);
                fixture.await(() -> slow.count() == 20 && fast.count() == 20, both);
                checkpoint = slow.snapshot();
                for (int i = 21; i <= total; i++) fixture.publish(i, payload, eagerPoll);
                fixture.awaitRecorded(eagerPoll);
                fixture.await(() -> slow.count() == total && fast.count() == total, both);
                requireSame(fast, audit);
                requireSame(fast, slow);
                System.out.printf("PASS composed projections matched; paused Archive follower caught up through %d%n", delayed.sequence());
            }
            CountingProjection restored = new CountingProjection(); restored.restore(checkpoint);
            EventHandler restoredHandler = restored::apply;
            try (CatalogSubscription reader = reader(b, fixture, 20, 4003)) {
                Runnable restoredPoll = () -> reader.poll(restoredHandler, 256);
                fixture.await(() -> restored.count() == total, restoredPoll);
                requireSame(fast, restored);
                System.out.printf("PASS application checkpoint replayed from 20 through %d%n", reader.sequence());
            }
        }
    }
    private static CatalogSubscription reader(AeronArchive archive, ArchiveFixture fixture, long sequence, int stream) {
        return new CatalogSubscription(archive, fixture.logId(), ArchiveFixture.LIVE_STREAM_ID, sequence,
            ArchiveFixture.REPLAY_CHANNEL, stream);
    }
    private static void requireSame(CountingProjection a, CountingProjection b) {
        if (!Arrays.equals(a.snapshot(), b.snapshot())) throw new IllegalStateException("projection state differs");
    }
}
