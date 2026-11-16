package io.github.jstadeusiak.spine.client;

import io.github.jstadeusiak.spine.protocol.sbe.SequencedEventDecoder;
import org.agrona.DirectBuffer;

import java.util.Objects;

/**
 * One synchronous application transition. The buffer and decoder are borrowed until return.
 * Handlers may consume the payload cursor; do not modify the bytes or rebind the shared decoder.
 */
@FunctionalInterface
public interface EventHandler {
    void onEvent(DirectBuffer buffer, int offset, int length, SequencedEventDecoder event);

    /** Compose during setup. Each handler starts at the payload; failure stops the chain without rolling back effects. */
    default EventHandler andThen(final EventHandler next) {
        Objects.requireNonNull(next, "next");
        return (buffer, offset, length, event) -> {
            event.sbeRewind();
            onEvent(buffer, offset, length, event);
            event.sbeRewind();
            next.onEvent(buffer, offset, length, event);
        };
    }
}
