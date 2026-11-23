package spine.sequencer;

import org.agrona.concurrent.UnsafeBuffer;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;

/** Independent test checksum, allowing structural corruption tests to get past the integrity fence. */
public final class SnapshotChecksum {
    private SnapshotChecksum() { }

    public static void seal(final UnsafeBuffer snapshot) {
        try {
            final byte[] bytes = new byte[snapshot.capacity()];
            snapshot.getBytes(0, bytes);
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, 0, 36);
            digest.update(bytes, 68, bytes.length - 68);
            snapshot.putBytes(36, digest.digest());
        } catch (final GeneralSecurityException failure) { throw new AssertionError(failure); }
    }
}
