package io.github.jstadeusiak.spine.protocol;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApplicationLimitsTest {
    @Test void declarationsPreserveSmallDefaultsAndAccountForCompleteSnapshotCapacity() {
        assertThat(ApplicationLimits.DEFAULT).isEqualTo(new ApplicationLimits(1024, 92, 2 * 1024 * 1024));
        final ApplicationLimits batch = new ApplicationLimits(8, 2 * 1024 * 1024, 32 * 1024 * 1024);
        assertThat(batch.snapshotBytes()).isEqualTo(72 + 8 * (68 + 2 * 1024 * 1024) + 4 + 32 * 1024 * 1024);
        assertThat(batch.eventBytes()).isEqualTo(Wire.MAX_EVENT_BYTES);
        assertThat(new ApplicationLimits(1, 8, 0).snapshotBytes()).isEqualTo(152);
    }

    @Test void invalidAndExcessiveAggregateCapacitiesFailBeforeAllocation() {
        for (final int[] invalid : new int[][]{
            {0, 8, 0}, {-1, 8, 0}, {1025, 8, 0}, {1, 7, 0}, {1, 2 * 1024 * 1024 + 1, 0},
            {1, 8, -1}, {1, 8, 64 * 1024 * 1024 + 1}, {1024, 2 * 1024 * 1024, 0},
            {64, 2 * 1024 * 1024, 0}, {Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE}
        }) {
            assertThatThrownBy(() -> new ApplicationLimits(invalid[0], invalid[1], invalid[2]))
                .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
