package spine.testkit;

import io.aeron.archive.client.AeronArchive;
import spine.client.CatalogSubscription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.*;

class RecoveryIntegrationTest {
    @TempDir Path directory;

    @Test @Timeout(60)
    void independentArchiveFollowersRecoverAnIdleTailAndAnOldApplicationSnapshot() {
        try (ArchiveFixture fixture = new ArchiveFixture(directory);
             AeronArchive a = fixture.connectArchive(); AeronArchive b = fixture.connectArchive()) {
            fixture.expectClosedConsumerControlResponse(a); fixture.expectClosedConsumerControlResponse(b);
            final CountingProjection eager = new CountingProjection(), slow = new CountingProjection();
            byte[] checkpoint;
            try (CatalogSubscription fast = reader(a, fixture, 0, 4101);
                 CatalogSubscription delayed = reader(b, fixture, 0, 4102)) {
                Runnable both = () -> { fast.poll(eager::apply, 256); delayed.poll(slow::apply, 256); };
                for (int i = 1; i <= 20; i++) fixture.publish(i, 4096, both);
                fixture.await(() -> eager.count() == 20 && slow.count() == 20, both);
                checkpoint = slow.snapshot();
                Runnable fastOnly = () -> fast.poll(eager::apply, 256);
                for (int i = 21; i <= 400; i++) fixture.publish(i, 4096, fastOnly);
                fixture.awaitRecorded(fastOnly);
                fixture.await(() -> eager.count() == 400, fastOnly);
                assertThat(slow.count()).isEqualTo(20);
                fixture.await(() -> slow.count() == 400, both);
                assertThat(slow.snapshot()).isEqualTo(eager.snapshot());
                assertThat(delayed.sequence()).isEqualTo(400);
            }
            CountingProjection restored = new CountingProjection(); restored.restore(checkpoint);
            try (CatalogSubscription resumed = reader(b, fixture, 20, 4103)) {
                fixture.await(() -> restored.count() == 400, () -> resumed.poll(restored::apply, 256));
                assertThat(restored.snapshot()).isEqualTo(eager.snapshot());
            }
        }
    }

    @Test @Timeout(30)
    void applicationFailureLeavesCursorAndBorrowedArchiveAvailableForRecovery() {
        try (ArchiveFixture fixture = new ArchiveFixture(directory); AeronArchive archive = fixture.connectArchive()) {
            fixture.expectClosedConsumerControlResponse(archive);
            fixture.publish(1, 4096, () -> {}); fixture.awaitRecorded(() -> {});
            try (CatalogSubscription reader = reader(archive, fixture, 0, 4201)) {
                boolean[] failed = {false};
                fixture.await(() -> failed[0], () -> {
                    try { reader.poll((buffer, offset, length, event) -> { throw new IllegalStateException("application failed"); }, 256); }
                    catch (RuntimeException expected) { failed[0] = true; }
                });
                assertThat(reader.sequence()).isZero();
                assertThat(archive.getRecordingPosition(fixture.recordingId())).isPositive();
            }
            CountingProjection recovered = new CountingProjection();
            try (CatalogSubscription replacement = reader(archive, fixture, 0, 4202)) {
                fixture.await(() -> recovered.count() == 1, () -> replacement.poll(recovered::apply, 256));
            }
        }
    }
    private static CatalogSubscription reader(AeronArchive archive, ArchiveFixture fixture, long sequence, int stream) {
        return new CatalogSubscription(archive, fixture.logId(), ArchiveFixture.LIVE_STREAM_ID, sequence,
            ArchiveFixture.REPLAY_CHANNEL, stream);
    }
}
