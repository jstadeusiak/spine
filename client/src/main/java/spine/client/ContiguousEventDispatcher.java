package spine.client;

import spine.protocol.Wire;
import org.agrona.DirectBuffer;

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Single-owner application boundary; hashing and all duplicate storage are preallocated. */
final class ContiguousEventDispatcher {
    static final int WINDOW = 1024;
    private final long logId;
    private final Wire wire = new Wire();
    private final MessageDigest digest = newDigest();
    private final byte[] scratch = new byte[Wire.MAX_EVENT_BYTES];
    private final byte[] fingerprint = new byte[32];
    private final byte[] recent = new byte[WINDOW * 32];
    private final long[] sequences = new long[WINDOW];
    private long sequence;

    ContiguousEventDispatcher(final long logId, final long lastApplied) {
        if (logId == 0 || logId == -1 || lastApplied < 0) { throw new IllegalArgumentException("invalid checkpoint"); }
        this.logId = logId;
        sequence = lastApplied;
        digest.update(scratch); // Initialize provider compression workspace before readiness.
        finishDigest();
    }

    void onMessage(final DirectBuffer buffer, final int offset, final int length, final EventHandler handler,
                   final boolean verifiedHistoricalPrefix) {
        if (!wire.validEvent(buffer, offset, length) || wire.event.logId() != logId) {
            throw new IllegalStateException("invalid event or foreign log");
        }
        final long next = wire.event.sequence();
        final int slot = (int)(next & (WINDOW - 1));
        if (next <= sequence && sequences[slot] != next) {
            if (verifiedHistoricalPrefix) { return; }
            throw new IllegalStateException("duplicate outside verification window");
        }
        buffer.getBytes(offset, scratch, 0, length);
        digest.update(scratch, 0, length);
        finishDigest();
        if (next <= sequence) {
            for (int i = 0; i < 32; i++) {
                if (recent[slot * 32 + i] != fingerprint[i]) { throw new IllegalStateException("conflicting duplicate"); }
            }
            return;
        }
        if (sequence == Long.MAX_VALUE || next != sequence + 1) { throw new IllegalStateException("canonical gap"); }
        handler.onEvent(buffer, offset, length, wire.event);
        System.arraycopy(fingerprint, 0, recent, slot * 32, 32);
        sequences[slot] = next;
        sequence = next;
    }

    long sequence() { return sequence; }

    private void finishDigest() {
        try { digest.digest(fingerprint, 0, 32); }
        catch (final DigestException error) { throw new AssertionError(error); }
    }

    static MessageDigest newDigest() {
        try { return MessageDigest.getInstance("SHA-256", "SUN"); }
        catch (final NoSuchAlgorithmException | java.security.NoSuchProviderException error) { throw new AssertionError(error); }
    }
}
