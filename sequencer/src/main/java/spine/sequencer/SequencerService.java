package spine.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import spine.protocol.ApplicationIdentity;
import spine.protocol.ApplicationLimits;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

/** One committed input, deterministic memory, ordinary recorded output on every member and during replay. */
public final class SequencerService implements ClusteredService {
    public static final String OUTPUT_CHANNEL = "aeron:ipc?term-length=1m|alias=spine-output";
    public static final int OUTPUT_STREAM = 1001;
    private static final long TIMEOUT_NS = 10_000_000_000L;
    private final SequencerCore core;
    private final ApplicationIdentity identity;
    private final String channel;
    private final int stream;
    private Cluster cluster;
    private ExclusivePublication output;
    private CountersReader counters;
    private int recordingCounter = CountersReader.NULL_COUNTER_ID;
    private long recordingId;
    private long recordingRegistration;
    private long publishedPosition;

    public SequencerService() { this(OUTPUT_CHANNEL, OUTPUT_STREAM); }
    public SequencerService(final String channel, final int stream) {
        this(ApplicationIdentity.PASS_THROUGH, Application.PASS_THROUGH, channel, stream);
    }
    public SequencerService(final ApplicationIdentity identity, final Application application) {
        this(identity, application, OUTPUT_CHANNEL, OUTPUT_STREAM);
    }
    public SequencerService(final ApplicationIdentity identity, final Application application,
        final String channel, final int stream) {
        this(identity, application, ApplicationLimits.DEFAULT, channel, stream);
    }
    public SequencerService(final ApplicationIdentity identity, final Application application,
        final ApplicationLimits limits, final String channel, final int stream) {
        this.identity = identity;
        core = new SequencerCore(identity, application, limits);
        this.channel = channel;
        this.stream = stream;
    }

    /** Configure both native recovery guards before launching either Cluster component. */
    public void configure(final ConsensusModule.Context consensus, final ClusteredServiceContainer.Context service) {
        consensus.appVersion(identity.version()).appVersionValidator((current, previous) -> current == previous);
        service.appVersion(identity.version()).appVersionValidator((current, previous) -> current == previous);
    }

    @Override public void onStart(final Cluster cluster, final Image snapshot) {
        if (cluster.context().appVersion() != identity.version()) {
            throw new AgentTerminationException("native application version mismatch");
        }
        this.cluster = cluster;
        try {
            try (AeronArchive archive = AeronArchive.connect(cluster.context().archiveContext().clone()
                .aeron(cluster.aeron()).ownsAeronClient(false))) {
                output = archive.addRecordedExclusivePublication(channel, stream);
                if (output.maxMessageLength() < core.limits().eventBytes()) {
                    throw new AgentTerminationException("output publication is too small for declared application results");
                }
                counters = cluster.aeron().countersReader();
                final long deadline = System.nanoTime() + TIMEOUT_NS;
                cluster.idleStrategy().reset();
                while ((recordingCounter = RecordingPos.findCounterIdBySession(counters, output.sessionId(),
                    archive.archiveId())) == CountersReader.NULL_COUNTER_ID
                    || (recordingRegistration = counters.getCounterRegistrationId(recordingCounter))
                        == CountersReader.DEFAULT_REGISTRATION_ID) {
                    if (System.nanoTime() - deadline >= 0) { throw new AgentTerminationException("output recorder absent"); }
                    cluster.idleStrategy().idle();
                }
                recordingId = RecordingPos.getRecordingId(counters, recordingCounter);
            }
            if (snapshot != null) {
                final UnsafeBuffer bytes = new UnsafeBuffer(new byte[core.limits().snapshotBytes()]);
                final int[] size = {0};
                final io.aeron.logbuffer.FragmentHandler reader = (buffer, offset, length, header) -> {
                    if (length > bytes.capacity() - size[0]) {
                        throw new AgentTerminationException("snapshot exceeds declared capacity");
                    }
                    bytes.putBytes(size[0], buffer, offset, length);
                    size[0] += length;
                };
                while (!snapshot.isEndOfStream()) {
                    if (snapshot.isClosed()) { throw new AgentTerminationException("incomplete core snapshot"); }
                    cluster.idleStrategy().idle(snapshot.poll(reader, 10));
                }
                core.restore(bytes, 0, size[0]);
            }
        } catch (final RuntimeException | Error failure) {
            CloseHelper.quietClose(output);
            throw failure;
        }
    }

    @Override public void onSessionMessage(final ClientSession session, final long timestamp,
        final DirectBuffer buffer, final int offset, final int length, final Header header) {
        core.onCommand(buffer, offset, length, timestamp, cluster.timeUnit().toNanos(1));
        if (core.eventLength() != 0) {
            publishedPosition = offer(output, core.resultBuffer(), core.eventOffset(), core.eventLength());
        }
        if (session != null) { session.offer(core.resultBuffer(), 0, core.resultLength()); }
    }

    /** A compact state snapshot is accepted only after this member's complete output fence. */
    @Override public void onTakeSnapshot(final ExclusivePublication publication) {
        awaitRecorded();
        final int length = core.snapshot();
        for (int offset = 0; offset < length;) {
            final int chunk = Math.min(length - offset, publication.maxMessageLength());
            offer(publication, core.resultBuffer(), offset, chunk);
            offset += chunk;
        }
    }

    private long offer(final ExclusivePublication publication, final DirectBuffer buffer,
        final int offset, final int length) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        cluster.idleStrategy().reset();
        long position;
        while ((position = publication.offer(buffer, offset, length)) <= 0) {
            if ((position != Publication.BACK_PRESSURED && position != Publication.ADMIN_ACTION)
                || System.nanoTime() - deadline >= 0) {
                throw new AgentTerminationException("recorded publication unavailable");
            }
            cluster.idleStrategy().idle();
        }
        return position;
    }

    private void awaitRecorded() {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        cluster.idleStrategy().reset();
        while (true) {
            if (!RecordingPos.isActive(counters, recordingCounter, recordingId)
                || counters.getCounterRegistrationId(recordingCounter) != recordingRegistration) {
                throw new AgentTerminationException("output recording stopped before snapshot fence");
            }
            final long recorded = counters.getCounterValue(recordingCounter);
            if (recorded >= publishedPosition && RecordingPos.isActive(counters, recordingCounter, recordingId)
                && counters.getCounterRegistrationId(recordingCounter) == recordingRegistration) { return; }
            if (System.nanoTime() - deadline >= 0) { throw new AgentTerminationException("output recording fence timeout"); }
            cluster.idleStrategy().idle();
        }
    }

    @Override public void onTerminate(final Cluster cluster) {
        try {
            awaitRecorded();
        } finally {
            CloseHelper.close(output);
        }
    }
    @Override public void onSessionOpen(final ClientSession session, final long timestamp) { }
    @Override public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason reason) { }
    @Override public void onTimerEvent(final long correlationId, final long timestamp) { }
    @Override public void onRoleChange(final Cluster.Role role) { }
}
