package io.github.jstadeusiak.spine.client;

import io.github.jstadeusiak.spine.protocol.Wire;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContiguousEventDispatcherTest {
    private final Wire wire = new Wire();
    private final UnsafeBuffer payload = new UnsafeBuffer(new byte[32]);
    private final UnsafeBuffer event = new UnsafeBuffer(new byte[Wire.EVENT_OVERHEAD + 32]);

    @Test void exactDuplicatesSkipButChangedBytesAndGapFailBeforeApplication() {
        final ContiguousEventDispatcher dispatcher = new ContiguousEventDispatcher(7, 0);
        final long[] applied = {0};
        final EventHandler handler = (buffer, offset, length, decoded) -> applied[0]++;
        final int length = event(1);
        dispatcher.onMessage(event, 0, length, handler, false);
        dispatcher.onMessage(event, 0, length, handler, false);
        assertThat(applied[0]).isEqualTo(1);
        event.putByte(Wire.EVENT_OVERHEAD, (byte)1);
        assertThatThrownBy(() -> dispatcher.onMessage(event, 0, length, handler, false))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("conflicting");
        event(3);
        assertThatThrownBy(() -> dispatcher.onMessage(event, 0, length, handler, false))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("gap");
        assertThat(dispatcher.sequence()).isEqualTo(1);
    }

    @Test void applicationFailureDoesNotAdvanceItsCursor() {
        final ContiguousEventDispatcher dispatcher = new ContiguousEventDispatcher(7, 0);
        final int length = event(1);
        assertThatThrownBy(() -> dispatcher.onMessage(event, 0, length, (buffer, offset, bytes, decoded) -> {
            throw new IllegalArgumentException("application failed");
        }, false)).isInstanceOf(IllegalArgumentException.class);
        assertThat(dispatcher.sequence()).isZero();
    }

    @Test void opaqueCheckpointPrefixNeedsVerifiedArchiveHistory() {
        final ContiguousEventDispatcher dispatcher = new ContiguousEventDispatcher(7, 2048);
        final EventHandler handler = (buffer, offset, length, decoded) -> { throw new AssertionError("prefix applied"); };
        final int length = event(1);
        assertThatThrownBy(() -> dispatcher.onMessage(event, 0, length, handler, false))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("verification window");
        dispatcher.onMessage(event, 0, length, handler, true);
        assertThat(dispatcher.sequence()).isEqualTo(2048);
    }

    private int event(final long sequence) {
        return wire.encodeEvent(event, 0, 7, sequence, 123, 1, 1, 1, 100, 101, 102, payload, 0, 32);
    }
}
