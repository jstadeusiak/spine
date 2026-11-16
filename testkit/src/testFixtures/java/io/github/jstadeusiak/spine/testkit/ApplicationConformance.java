package io.github.jstadeusiak.spine.testkit;

import io.github.jstadeusiak.spine.protocol.ApplicationIdentity;
import io.github.jstadeusiak.spine.protocol.ApplicationLimits;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import io.github.jstadeusiak.spine.sequencer.Application;
import io.github.jstadeusiak.spine.sequencer.SequencerCore;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Supplier;

/** Cold, allocating test fixture. A corpus contains exact encoded proposals and logged nanosecond timestamps. */
public final class ApplicationConformance {
    public record Command(byte[] bytes, long loggedTimeNanos) { }
    public record Report(int commands, int snapshotCuts, String canonicalSha256, String finalSnapshotSha256) { }
    private record Trace(AdmissionStatus[] statuses, byte[][] receipts, byte[][] events, byte[][] snapshots) { }

    private ApplicationConformance() { }

    /** Check repeat execution, every restore cut, exact retries and rejected snapshot atomicity. */
    public static Report verify(final Supplier<? extends Application> factory, final ApplicationIdentity identity,
        final long logId, final List<Command> commands) {
        return verify(factory, identity, ApplicationLimits.DEFAULT, logId, commands);
    }

    /** Check an application using its declared producer, result and state bounds. */
    public static Report verify(final Supplier<? extends Application> factory, final ApplicationIdentity identity,
        final ApplicationLimits limits, final long logId, final List<Command> commands) {
        final Trace expected = trace(factory, identity, limits, logId, commands);
        final Trace repeated = trace(factory, identity, limits, logId, commands);
        require(Arrays.equals(expected.statuses, repeated.statuses), "repeated statuses differ");
        require(Arrays.deepEquals(expected.receipts, repeated.receipts), "repeated receipts differ");
        require(Arrays.deepEquals(expected.events, repeated.events), "repeated canonical bytes differ");
        require(Arrays.deepEquals(expected.snapshots, repeated.snapshots), "repeated snapshots differ");
        for (int cut = 0; cut <= commands.size(); cut++) {
            final SequencerCore restored = new SequencerCore(identity, factory.get(), limits);
            final byte[] saved = expected.snapshots[cut];
            restored.restore(new UnsafeBuffer(saved), 0, saved.length);
            equal(saved, snapshot(restored), "restored snapshot differs at cut " + cut);
            rejectedSnapshotsLeaveState(restored, saved);
            int previous = cut - 1;
            while (previous >= 0 && expected.statuses[previous] != AdmissionStatus.ACCEPTED) { previous--; }
            if (previous >= 0) { retry(restored, commands.get(previous), expected.events[previous], saved); }
            for (int index = cut; index < commands.size(); index++) {
                require(send(restored, commands.get(index)) == expected.statuses[index], "suffix admission differs");
                equal(expected.receipts[index], bytes(restored.resultBuffer(), 0, restored.resultLength()), "suffix receipt differs");
                equal(expected.events[index], bytes(restored.resultBuffer(), restored.eventOffset(), restored.eventLength()),
                    "suffix canonical output differs");
                final byte[] state = snapshot(restored);
                equal(expected.snapshots[index + 1], state, "suffix state differs");
                if (expected.statuses[index] == AdmissionStatus.ACCEPTED) {
                    retry(restored, commands.get(index), expected.events[index], state);
                }
            }
        }
        return new Report(commands.size(), commands.size() + 1, digest(expected.events),
            digest(new byte[][]{expected.snapshots[commands.size()]}));
    }

    private static Trace trace(final Supplier<? extends Application> factory, final ApplicationIdentity identity,
        final ApplicationLimits limits, final long logId, final List<Command> commands) {
        final SequencerCore core = new SequencerCore(identity, factory.get(), limits);
        final Wire wire = new Wire();
        final UnsafeBuffer initialize = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        require(core.onCommand(initialize, 0, wire.encodeInitialize(initialize, 0, logId, identity), 0, 1)
            == AdmissionStatus.INITIALIZED, "initialization failed");
        final AdmissionStatus[] statuses = new AdmissionStatus[commands.size()];
        final byte[][] receipts = new byte[commands.size()][], events = new byte[commands.size()][];
        final byte[][] snapshots = new byte[commands.size() + 1][];
        snapshots[0] = snapshot(core);
        for (int i = 0; i < commands.size(); i++) {
            statuses[i] = send(core, commands.get(i));
            receipts[i] = bytes(core.resultBuffer(), 0, core.resultLength());
            events[i] = bytes(core.resultBuffer(), core.eventOffset(), core.eventLength());
            snapshots[i + 1] = snapshot(core);
            if (statuses[i] == AdmissionStatus.ACCEPTED) { retry(core, commands.get(i), events[i], snapshots[i + 1]); }
            else { equal(snapshots[i], snapshots[i + 1], "non-admitted input changed state"); }
        }
        return new Trace(statuses, receipts, events, snapshots);
    }

    private static AdmissionStatus send(final SequencerCore core, final Command command) {
        return core.onCommand(new UnsafeBuffer(command.bytes), 0, command.bytes.length, command.loggedTimeNanos, 1);
    }

    private static void retry(final SequencerCore core, final Command command, final byte[] canonical, final byte[] state) {
        final long differentTime = command.loggedTimeNanos == Long.MAX_VALUE ? 0 : command.loggedTimeNanos + 1;
        require(core.onCommand(new UnsafeBuffer(command.bytes), 0, command.bytes.length, differentTime, 1)
            == AdmissionStatus.DUPLICATE && core.eventLength() == 0, "exact retry repeated admission");
        equal(canonical, bytes(core.resultBuffer(), core.eventOffset(), core.resultLength() - Wire.RESULT_OVERHEAD),
            "retry result differs from original canonical bytes");
        equal(state, snapshot(core), "retry changed state");
    }

    private static void rejectedSnapshotsLeaveState(final SequencerCore core, final byte[] saved) {
        final byte[] corrupt = saved.clone();
        corrupt[corrupt.length - 1] ^= 1;
        for (final byte[] bad : new byte[][]{Arrays.copyOf(saved, saved.length - 1), corrupt}) {
            boolean rejected = false;
            try { core.restore(new UnsafeBuffer(bad), 0, bad.length); }
            catch (final IllegalArgumentException expected) { rejected = true; }
            require(rejected, "invalid snapshot was accepted");
            equal(saved, snapshot(core), "invalid snapshot changed live state");
        }
    }

    private static byte[] snapshot(final SequencerCore core) { return bytes(core.resultBuffer(), 0, core.snapshot()); }

    private static byte[] bytes(final DirectBuffer input, final int offset, final int length) {
        final byte[] bytes = new byte[length];
        input.getBytes(offset, bytes);
        return bytes;
    }

    private static String digest(final byte[][] frames) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (final byte[] frame : frames) {
                for (int shift = 24; shift >= 0; shift -= 8) { digest.update((byte)(frame.length >>> shift)); }
                digest.update(frame);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (final java.security.NoSuchAlgorithmException unavailable) { throw new AssertionError(unavailable); }
    }

    private static void equal(final byte[] expected, final byte[] actual, final String message) {
        require(Arrays.equals(expected, actual), message);
    }

    private static void require(final boolean valid, final String message) {
        if (!valid) { throw new AssertionError(message); }
    }
}
