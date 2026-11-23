package spine.sequencer;

import io.aeron.archive.ControlSessionCounter;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.exceptions.AeronException;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClosedControlSessionTest {
    private static final long SESSION = 123, ARCHIVE = 7;
    private static final String NAME = "consensus-module";
    private final CountersManager counters = new CountersManager(
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(4 * CountersReader.METADATA_LENGTH, 64)),
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(4 * CountersReader.COUNTER_LENGTH, 64)));

    @Test void onlyTheReclaimedNativeSessionCanExplainItsInitialKeepAlive() {
        final int id = counter(ControlSessionCounter.TYPE_ID, SESSION, NAME, ARCHIVE);
        assertThat(matches(warning(SESSION, 61))).isFalse();
        counters.free(id);
        assertThat(matches(warning(SESSION, 61))).isTrue();
        assertThat(matches(warning(SESSION + 1, 61))).isFalse();
        assertThat(matches(warning(SESSION, 62))).isFalse();
        assertThat(matches(new IllegalStateException(warning(SESSION, 61).getMessage()))).isFalse();
        assertThat(matches(new ArchiveEvent("control request for unknown session: controlSessionId=123 templateId=61",
            AeronException.Category.ERROR))).isFalse();
    }

    @Test void otherCounterTypesClientsAndArchivesRemainFatal() {
        counters.free(counter(ControlSessionCounter.TYPE_ID + 1, SESSION, NAME, ARCHIVE));
        assertThat(matches(warning(SESSION, 61))).isFalse();
        counters.free(counter(ControlSessionCounter.TYPE_ID, SESSION, "test-client", ARCHIVE));
        assertThat(matches(warning(SESSION, 61))).isFalse();
        counters.free(counter(ControlSessionCounter.TYPE_ID, SESSION, NAME, ARCHIVE + 1));
        assertThat(matches(warning(SESSION, 61))).isFalse();
    }

    @Test void malformedOrUnrecordedSessionsRemainFatal() {
        assertThat(matches(warning(SESSION, 61))).isFalse();
        assertThat(matches(new ArchiveEvent("control request for unknown session: controlSessionId=invalid templateId=61")))
            .isFalse();
    }

    private int counter(final int type, final long session, final String name, final long archive) {
        final int id = counters.allocate("control-session: name=" + name + " version=1.53.3 - archiveId=" + archive,
            type, buffer -> { });
        counters.setCounterValue(id, session);
        return id;
    }

    private static ArchiveEvent warning(final long session, final int template) {
        return new ArchiveEvent("control request for unknown session: controlSessionId=" + session + " templateId=" + template);
    }

    private boolean matches(final Throwable failure) {
        return ClusterFixture.isClosedNativeKeepAlive(failure, counters, ARCHIVE, NAME);
    }
}
