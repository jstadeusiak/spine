package spine.testkit;

import spine.protocol.Wire;
import spine.testkit.counter.sbe.CounterCommandEncoder;
import spine.testkit.counter.sbe.MessageHeaderEncoder;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Runnable cold conformance example, also consumable from the published test-fixture artifact. */
public final class CounterExample {
    public static final long LOG_ID = 17;

    private CounterExample() { }

    public static void main(final String[] args) throws Exception {
        final ApplicationConformance.Report report = ApplicationConformance.verify(
            CounterApplication::new, CounterApplication.IDENTITY, LOG_ID, corpus());
        final String json = "{\"commands\":" + report.commands() + ",\"snapshotCuts\":" + report.snapshotCuts()
            + ",\"canonicalSha256\":\"" + report.canonicalSha256() + "\",\"finalSnapshotSha256\":\""
            + report.finalSnapshotSha256() + "\"}";
        if (args.length > 0) {
            final Path path = Path.of(args[0]).toAbsolutePath();
            Files.createDirectories(path.getParent());
            Files.writeString(path, json + "\n");
        }
        System.out.println(json);
    }

    /** Exact inputs include two producers, a duplicate, domain rejections and a protocol sequence gap. */
    public static List<ApplicationConformance.Command> corpus() {
        final ApplicationConformance.Command second = command(31, 2, 5, 101);
        return List.of(command(31, 1, 3, 100), second,
            new ApplicationConformance.Command(second.bytes().clone(), 999), command(41, 1, 7, 102),
            command(31, 3, -1, 103), command(31, 4, Long.MAX_VALUE, 104), command(31, 5, 8, 105),
            command(31, 7, 1, 106), command(31, 6, 2, 107));
    }

    private static ApplicationConformance.Command command(final long producer, final long sequence,
        final long delta, final long time) {
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.HEADER_LENGTH + CounterCommandEncoder.BLOCK_LENGTH]);
        new CounterCommandEncoder().wrapAndApplyHeader(payload, 0, new MessageHeaderEncoder()).delta(delta);
        final byte[] bytes = new byte[Wire.PROPOSAL_OVERHEAD + payload.capacity()];
        new Wire().encodeProposal(new UnsafeBuffer(bytes), 0, LOG_ID, producer, sequence, 1, 1, 1,
            90, 91, 92, payload, 0, payload.capacity());
        return new ApplicationConformance.Command(bytes, time);
    }
}
