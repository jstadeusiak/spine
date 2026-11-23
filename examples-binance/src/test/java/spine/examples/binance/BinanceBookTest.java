package spine.examples.binance;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class BinanceBookTest {
    private static volatile byte[] escapingAllocation;
    private static volatile long consumed;

    @Test
    void snapshotAndAbsoluteUpdatesPreserveExactCoverageAndBbo() {
        final BinanceBook book = initialized();
        assertThat(book.nativeFrontier).isEqualTo(100);
        assertThat(book.bidFloor).isEqualTo(90);
        assertThat(book.askCeiling).isEqualTo(120);
        assertThat(book.bidPrice).isEqualTo(100);
        assertThat(book.askPrice).isEqualTo(110);
        assertThat(book.eligible()).isTrue();
        final BinanceDepthDecoder update = update(99, 103, new long[]{100, 0, 99, 7, 89, 999},
            new long[]{110, 0, 111, 8, 121, 999});
        assertThat(book.apply(update, false, 200)).isEqualTo(BinanceBook.APPLIED);
        assertThat(book.nativeFrontier).isEqualTo(103);
        assertThat(book.bidPrice).isEqualTo(99);
        assertThat(book.bidSize).isEqualTo(7);
        assertThat(book.askPrice).isEqualTo(111);
        assertThat(book.askSize).isEqualTo(8);
        assertThat(book.bids.get(89)).isEqualTo(-1);
        assertThat(book.asks.get(121)).isEqualTo(-1);
        assertThat(book.lastBidCount).isEqualTo(3); // Ignored depth is still retained for comparable conflicts.
        assertThat(book.lastTimeNanos).isEqualTo(200);
        assertThat(book.apply(update(1, 102, new long[]{99, 99}, new long[0]), false, 201))
            .isEqualTo(BinanceBook.STALE);
        assertThat(book.bids.get(99)).isEqualTo(7);
    }

    @Test
    void cachedBboTracksQuantityInsertDeleteReplacementEmptyAndCrossedTransitions() {
        final BinanceBook book = initialized();
        assertThat(book.apply(update(101, 101, new long[]{100, 7}, new long[]{110, 8}), false, 201))
            .isEqualTo(BinanceBook.APPLIED);
        assertBbo(book, 100, 7, 110, 8);
        assertThat(book.apply(update(102, 102, new long[]{105, 2}, new long[]{108, 4}), false, 202))
            .isEqualTo(BinanceBook.APPLIED);
        assertBbo(book, 105, 2, 108, 4);
        assertThat(book.apply(update(103, 103, new long[]{105, 0, 104, 9}, new long[]{108, 0, 109, 10}), false, 203))
            .isEqualTo(BinanceBook.APPLIED);
        assertBbo(book, 104, 9, 109, 10);
        assertThat(book.apply(update(104, 104, new long[]{90, 0, 101, 6}, new long[]{120, 0, 119, 3}), false, 204))
            .isEqualTo(BinanceBook.APPLIED);
        assertBbo(book, 104, 9, 109, 10);
        assertThat(book.apply(update(105, 105, new long[]{104, 0}, new long[]{109, 0}), false, 205))
            .isEqualTo(BinanceBook.APPLIED);
        assertBbo(book, 101, 6, 110, 8);
        assertThat(book.apply(update(106, 106, new long[]{101, 0, 100, 0}, new long[]{110, 0, 119, 0}), false, 206))
            .isEqualTo(BinanceBook.APPLIED);
        assertBbo(book, 0, 0, 0, 0);
        assertThat(book.eligible()).isFalse();
        assertThat(book.apply(update(107, 107, new long[]{103, 15}, new long[]{112, 16}), false, 207))
            .isEqualTo(BinanceBook.APPLIED);
        assertBbo(book, 103, 15, 112, 16);
        assertThat(book.eligible()).isTrue();
        assertThat(book.apply(update(108, 108, new long[]{112, 1}, new long[0]), false, 208))
            .isEqualTo(BinanceBook.INVALID_INPUT);
        assertBbo(book, 112, 1, 112, 16);
        assertThat(book.eligible()).isFalse();
    }

    @Test
    void pendingRangesDrainInCommittedArrivalOrderAndRetainOriginalFrontierTime() {
        final BinanceBook book = initialized();
        assertThat(book.apply(update(104, 105, new long[]{100, 5}, new long[0]), false, 201))
            .isEqualTo(BinanceBook.PENDING);
        assertThat(book.apply(update(102, 104, new long[]{100, 4}, new long[0]), false, 202))
            .isEqualTo(BinanceBook.PENDING);
        assertThat(book.status).isEqualTo(BinanceBook.GAP);
        assertThat(book.eligible()).isFalse();
        assertThat(book.apply(update(101, 101, new long[]{100, 3}, new long[0]), false, 203))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.nativeFrontier).isEqualTo(105);
        assertThat(book.lastTimeNanos).isEqualTo(201);
        assertThat(book.bids.get(100)).isEqualTo(5);
        assertThat(book.pendingCount).isZero();
        assertThat(book.status).isEqualTo(BinanceBook.VALID);
        assertThat(book.eligible()).isTrue();
    }

    @Test
    void currentExactSpanComparisonIsOrderIndependentAndIncludesOutOfCoverageRows() {
        final BinanceBook book = initialized();
        assertThat(book.apply(update(101, 102, new long[]{100, 4, 89, 5}, new long[]{110, 6}), false, 200))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.apply(update(101, 102, new long[]{89, 5, 100, 4}, new long[]{110, 6}), false, 201))
            .isEqualTo(BinanceBook.STALE);
        // Another batching range can contain a consistent subset rather than identical changed rows.
        assertThat(book.apply(update(100, 102, new long[]{100, 4}, new long[0]), false, 202))
            .isEqualTo(BinanceBook.STALE);
        assertThat(book.apply(update(101, 102, new long[]{89, 7, 100, 4}, new long[]{110, 6}), false, 203))
            .isEqualTo(BinanceBook.CONFLICTING);
        assertThat(book.status).isEqualTo(BinanceBook.CONFLICT);
        assertThat(book.bids.get(100)).isEqualTo(4);
        assertThat(book.eligible()).isFalse();
        assertThat(book.apply(update(103, 103, new long[0], new long[0]), false, 204))
            .isEqualTo(BinanceBook.NEEDS_BOOTSTRAP);
        assertThat(book.nativeFrontier).isEqualTo(102);
        assertThat(book.apply(update(102, 102, new long[]{100, 1, 90, 2}, new long[]{110, 3, 120, 4}), true, 205))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.eligible()).isTrue();
    }

    @Test
    void differentlyBatchedSameFrontierUpdatesVerifyTheirCoveredAbsoluteQuantities() {
        final BinanceBook book = initialized();
        assertThat(book.apply(update(101, 103, new long[]{100, 4, 99, 0, 89, 5}, new long[]{110, 6}), false, 200))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.apply(update(100, 103, new long[]{100, 4, 99, 0}, new long[0]), false, 201))
            .isEqualTo(BinanceBook.STALE);
        // Prices outside retained bootstrap coverage cannot establish a contradiction for another range.
        assertThat(book.apply(update(100, 103, new long[]{89, 999}, new long[0]), false, 202))
            .isEqualTo(BinanceBook.STALE);
        assertThat(book.eligible()).isTrue();
        assertThat(book.apply(update(100, 103, new long[]{100, 7}, new long[0]), false, 203))
            .isEqualTo(BinanceBook.CONFLICTING);
        assertThat(book.nativeFrontier).isEqualTo(103);
        assertThat(book.bids.get(100)).isEqualTo(4);
        assertThat(book.status).isEqualTo(BinanceBook.CONFLICT);
        assertThat(book.eligible()).isFalse();
    }

    @Test
    void sameFrontierComparisonAlsoAppliesImmediatelyAfterBootstrap() {
        final BinanceBook book = initialized();
        assertThat(book.apply(update(99, 100, new long[]{100, 1, 99, 0}, new long[]{110, 3}), false, 200))
            .isEqualTo(BinanceBook.STALE);
        assertThat(book.eligible()).isTrue();
        // A zero-size row contradicts a retained live level even without a retained last delta.
        assertThat(book.apply(update(99, 100, new long[]{100, 0}, new long[0]), false, 201))
            .isEqualTo(BinanceBook.CONFLICTING);
        assertThat(book.bids.get(100)).isEqualTo(1);
        assertThat(book.eligible()).isFalse();
    }

    @Test
    void conflictingPendingDuplicateImmediatelyInvalidatesTheBook() {
        final BinanceBook book = initialized();
        assertThat(book.apply(update(102, 102, new long[]{100, 4}, new long[0]), false, 201))
            .isEqualTo(BinanceBook.PENDING);
        assertThat(book.apply(update(102, 102, new long[]{100, 5}, new long[0]), false, 202))
            .isEqualTo(BinanceBook.CONFLICTING);
        assertThat(book.status).isEqualTo(BinanceBook.CONFLICT);
        assertThat(book.eligible()).isFalse();
        assertThat(book.pendingCount).isEqualTo(1);
        assertThat(book.apply(update(101, 101, new long[0], new long[0]), false, 203))
            .isEqualTo(BinanceBook.NEEDS_BOOTSTRAP);
        assertThat(book.nativeFrontier).isEqualTo(100);
        assertThat(book.bids.get(100)).isEqualTo(1);
        assertThat(book.eligible()).isFalse();
    }

    @Test
    void redundantFutureStormUsesOnePendingSlotAndComparesRowsWithoutOrderDependence() {
        final BinanceBook book = initialized();
        final BinanceDepthDecoder first = update(102, 103, new long[]{100, 4, 99, 3}, new long[]{110, 5, 111, 6});
        final BinanceDepthDecoder redundant = update(102, 103, new long[]{99, 3, 100, 4},
            new long[]{111, 6, 110, 5});
        assertThat(book.apply(first, false, 201)).isEqualTo(BinanceBook.PENDING);
        for (int i = 0; i < 100; i++) {
            assertThat(book.apply(redundant, false, 202 + i)).isEqualTo(BinanceBook.PENDING);
        }
        assertThat(book.pendingCount).isEqualTo(1);
        assertThat(book.status).isEqualTo(BinanceBook.GAP);
        assertThat(book.apply(update(101, 101, new long[0], new long[0]), false, 400))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.nativeFrontier).isEqualTo(103);
        assertThat(book.bids.get(100)).isEqualTo(4);
        assertThat(book.asks.get(111)).isEqualTo(6);
        assertThat(book.lastTimeNanos).isEqualTo(201);
        assertThat(book.pendingCount).isZero();
        assertThat(book.eligible()).isTrue();
    }

    @Test
    void pendingBoundsAndTerminalStatesRequireBootstrap() {
        final BinanceBook book = initialized();
        for (int i = 0; i < BinanceBook.MAX_PENDING; i++) {
            assertThat(book.apply(update(102 + i, 102 + i, new long[0], new long[0]), false, 200 + i))
                .isEqualTo(BinanceBook.PENDING);
        }
        assertThat(book.apply(update(110, 110, new long[0], new long[0]), false, 210))
            .isEqualTo(BinanceBook.NEEDS_BOOTSTRAP);
        assertThat(book.status).isEqualTo(BinanceBook.CAPACITY);
        assertThat(book.pendingCount).isEqualTo(BinanceBook.MAX_PENDING);
        assertThat(book.nativeFrontier).isEqualTo(100);
        assertThat(book.apply(update(101, 101, new long[0], new long[0]), false, 211))
            .isEqualTo(BinanceBook.NEEDS_BOOTSTRAP);

        final BinanceBook rowLimited = initialized();
        final long[] rows = new long[(BinanceBook.MAX_PENDING_LEVELS + 1) * 2];
        for (int i = 0; i < rows.length / 2; i++) {
            rows[i * 2] = 1 + i;
            rows[i * 2 + 1] = 1;
        }
        assertThat(rowLimited.apply(update(102, 102, rows, new long[0]), false, 200))
            .isEqualTo(BinanceBook.NEEDS_BOOTSTRAP);
        assertThat(rowLimited.status).isEqualTo(BinanceBook.CAPACITY);
        assertThat(rowLimited.pendingCount).isZero();
    }

    @Test
    void capacityValidationIsAtomicAcrossBothSidesAndAllowsDeletesBeforeInserts() {
        final BinanceBook book = new BinanceBook();
        final long[] bids = new long[BinanceBook.MAX_LEVELS * 2];
        for (int i = 0; i < BinanceBook.MAX_LEVELS; i++) {
            bids[i * 2] = i + 1;
            bids[i * 2 + 1] = 1;
        }
        assertThat(book.apply(update(100, 100, bids, new long[]{10_000, 1}), true, 100))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.apply(update(101, 101, new long[]{9_000, 2}, new long[]{10_000, 9}), false, 200))
            .isEqualTo(BinanceBook.NEEDS_BOOTSTRAP);
        assertThat(book.status).isEqualTo(BinanceBook.CAPACITY);
        assertThat(book.nativeFrontier).isEqualTo(100);
        assertThat(book.asks.get(10_000)).isEqualTo(1);
        assertThat(book.bids.get(9_000)).isEqualTo(-1);
        assertThat(book.bids.size()).isEqualTo(BinanceBook.MAX_LEVELS);
        assertThat(book.apply(update(100, 100, bids, new long[]{10_000, 1}), true, 201))
            .isEqualTo(BinanceBook.APPLIED);
        // Insertion precedes the deletion in the incoming rows, but mutation deletes first.
        assertThat(book.apply(update(101, 101, new long[]{9_000, 2, 1, 0}, new long[0]), false, 202))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.bids.get(1)).isEqualTo(-1);
        assertThat(book.bids.get(9_000)).isEqualTo(2);
        assertThat(book.bids.size()).isEqualTo(BinanceBook.MAX_LEVELS);
    }

    @Test
    void incompleteOrCrossedBooksAreNeverEligibleAndInvalidSnapshotsDoNotReplaceLevels() {
        final BinanceBook empty = new BinanceBook();
        assertThat(empty.apply(update(1, 1, new long[0], new long[0]), false, 1))
            .isEqualTo(BinanceBook.NEEDS_BOOTSTRAP);
        assertThat(empty.apply(update(100, 100, new long[0], new long[]{110, 2}), true, 2))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(empty.bidFloor).isZero();
        assertThat(empty.eligible()).isFalse();
        assertThat(empty.apply(update(101, 101, new long[]{100, 2}, new long[0]), false, 3))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(empty.bids.size()).isZero();
        assertThat(empty.eligible()).isFalse();

        final BinanceBook book = initialized();
        assertThat(book.apply(update(101, 101, new long[]{120, 1}, new long[]{110, 2}), true, 200))
            .isEqualTo(BinanceBook.INVALID_INPUT);
        assertThat(book.bids.get(100)).isEqualTo(1);
        assertThat(book.nativeFrontier).isEqualTo(100);
        assertThat(book.status).isEqualTo(BinanceBook.INVALID);
        assertThat(book.eligible()).isFalse();
        assertThat(book.apply(update(100, 100, new long[]{100, 1, 90, 2}, new long[]{110, 3, 120, 4}), true, 201))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.apply(update(101, 101, new long[]{111, 1}, new long[0]), false, 202))
            .isEqualTo(BinanceBook.INVALID_INPUT);
        assertThat(book.nativeFrontier).isEqualTo(101);
        assertThat(book.bids.get(111)).isEqualTo(1);
        assertThat(book.eligible()).isFalse();
    }

    @Test
    void longMaxFrontierHasNoSignedOverflowAndMalformedRowsAreAtomic() {
        final BinanceBook book = initialized();
        assertThat(book.apply(update(101, Long.MAX_VALUE, new long[0], new long[0]), false, 200))
            .isEqualTo(BinanceBook.APPLIED);
        assertThat(book.apply(update(Long.MAX_VALUE, Long.MAX_VALUE, new long[0], new long[0]), false, 201))
            .isEqualTo(BinanceBook.STALE);
        assertThat(book.nativeFrontier).isEqualTo(Long.MAX_VALUE);
        final BinanceDepthDecoder invalid = update(1, Long.MAX_VALUE, new long[]{100, 9, 90, -1}, new long[0]);
        assertThat(book.apply(invalid, false, 202)).isEqualTo(BinanceBook.INVALID_INPUT);
        assertThat(book.bids.get(100)).isEqualTo(1);
        assertThat(book.status).isEqualTo(BinanceBook.INVALID);
    }

    @Test
    void randomAbsoluteUpdatesMatchIndependentOrderedMapOracleAfterEveryInput() {
        final BinanceBook book = initialized();
        final TreeMap<Long, Long> bids = new TreeMap<>(Map.of(90L, 2L, 100L, 1L));
        final TreeMap<Long, Long> asks = new TreeMap<>(Map.of(110L, 3L, 120L, 4L));
        final Random random = new Random(823_041);
        for (int i = 0; i < 2_000; i++) {
            final long bidPrice = 85 + random.nextInt(21);
            final long askPrice = 106 + random.nextInt(20);
            final long bidSize = random.nextInt(100);
            final long askSize = random.nextInt(100);
            assertThat(book.apply(update(101 + i, 101 + i, new long[]{bidPrice, bidSize},
                new long[]{askPrice, askSize}), false, 200 + i)).isEqualTo(BinanceBook.APPLIED);
            oracleUpdate(bids, bidPrice, bidSize, bidPrice >= 90);
            oracleUpdate(asks, askPrice, askSize, askPrice <= 120);
            assertThat(book.bids.size()).isEqualTo(bids.size());
            assertThat(book.asks.size()).isEqualTo(asks.size());
            for (final Map.Entry<Long, Long> row : bids.entrySet()) {
                assertThat(book.bids.get(row.getKey().longValue())).isEqualTo(row.getValue());
            }
            for (final Map.Entry<Long, Long> row : asks.entrySet()) {
                assertThat(book.asks.get(row.getKey().longValue())).isEqualTo(row.getValue());
            }
            assertThat(book.bidPrice).isEqualTo(bids.isEmpty() ? 0 : bids.lastKey());
            assertThat(book.askPrice).isEqualTo(asks.isEmpty() ? 0 : asks.firstKey());
            assertThat(book.bidSize).isEqualTo(bids.isEmpty() ? 0 : bids.get(bids.lastKey()));
            assertThat(book.askSize).isEqualTo(asks.isEmpty() ? 0 : asks.get(asks.firstKey()));
            assertThat(book.eligible()).isEqualTo(!bids.isEmpty() && !asks.isEmpty());
        }
    }

    @Test
    void mutationRetriesRejectionsAndGapRepairHaveZeroWarmedAllocation() {
        final ThreadMXBean bean = allocationAccounting();
        final BinanceBook book = initialized();
        final BinanceDepthDecoder update = update(101, 101, new long[]{100, 5}, new long[]{110, 6});
        final Runnable mutation = () -> {
            update.firstId = update.lastId = book.nativeFrontier + 1;
            update.bidSizes[0] = 5 + (update.lastId & 1);
            require(book.apply(update, false, update.lastId), BinanceBook.APPLIED);
            require(book.apply(update, false, update.lastId), BinanceBook.STALE);
            consumed ^= book.bidSize;
        };
        noAllocation(bean, mutation);

        final BinanceDepthDecoder later = update(1, 1, new long[0], new long[0]);
        final BinanceDepthDecoder earlier = update(1, 1, new long[0], new long[0]);
        final BinanceDepthDecoder repair = update(1, 1, new long[0], new long[0]);
        final Runnable gaps = () -> {
            later.firstId = later.lastId = book.nativeFrontier + 3;
            earlier.firstId = earlier.lastId = book.nativeFrontier + 2;
            repair.firstId = repair.lastId = book.nativeFrontier + 1;
            require(book.apply(later, false, later.lastId), BinanceBook.PENDING);
            require(book.apply(later, false, later.lastId), BinanceBook.PENDING);
            require(book.apply(earlier, false, earlier.lastId), BinanceBook.PENDING);
            require(book.apply(repair, false, repair.lastId), BinanceBook.APPLIED);
            consumed ^= book.nativeFrontier;
        };
        noAllocation(bean, gaps);
        update.firstId = update.lastId = book.nativeFrontier + 1;
        update.bidSizes[0] = -1;
        final Runnable invalid = () -> {
            require(book.apply(update, false, 1), BinanceBook.INVALID_INPUT);
            consumed ^= book.status;
        };
        noAllocation(bean, invalid);
        final long negative = allocated(bean, () -> escapingAllocation = new byte[64], 1_024);
        assertThat(negative).as("escaping allocation proves active accounting").isGreaterThanOrEqualTo(65_536);
        escapingAllocation = null;
    }

    private static void oracleUpdate(final TreeMap<Long, Long> levels, final long price, final long size,
        final boolean covered) {
        if (covered) {
            if (size == 0) {
                levels.remove(price);
            } else {
                levels.put(price, size);
            }
        }
    }

    private static void assertBbo(final BinanceBook book, final long bidPrice, final long bidSize,
        final long askPrice, final long askSize) {
        assertThat(book.bidPrice).isEqualTo(bidPrice);
        assertThat(book.bidSize).isEqualTo(bidSize);
        assertThat(book.askPrice).isEqualTo(askPrice);
        assertThat(book.askSize).isEqualTo(askSize);
    }

    private static BinanceBook initialized() {
        final BinanceBook book = new BinanceBook();
        assertThat(book.apply(update(100, 100, new long[]{100, 1, 90, 2}, new long[]{110, 3, 120, 4}), true, 100))
            .isEqualTo(BinanceBook.APPLIED);
        return book;
    }

    private static BinanceDepthDecoder update(final long first, final long last, final long[] bids,
        final long[] asks) {
        final BinanceDepthDecoder update = new BinanceDepthDecoder();
        update.firstId = first;
        update.lastId = last;
        update.bidCount = bids.length / 2;
        update.askCount = asks.length / 2;
        for (int i = 0; i < update.bidCount; i++) {
            update.bidPrices[i] = bids[i * 2];
            update.bidSizes[i] = bids[i * 2 + 1];
        }
        for (int i = 0; i < update.askCount; i++) {
            update.askPrices[i] = asks[i * 2];
            update.askSizes[i] = asks[i * 2 + 1];
        }
        return update;
    }

    private static void require(final int actual, final int expected) {
        if (actual != expected) {
            throw new AssertionError("unexpected book outcome: " + actual);
        }
    }

    private static void noAllocation(final ThreadMXBean bean, final Runnable action) {
        for (int i = 0; i < 100_000; i++) {
            action.run();
        }
        allocated(bean, action, 32);
        assertThat(allocated(bean, action, 10_000)).isZero();
        assertThat(allocated(bean, action, 20_000)).isZero();
    }

    private static long allocated(final ThreadMXBean bean, final Runnable action, final int count) {
        final long id = Thread.currentThread().threadId();
        final long before = bean.getThreadAllocatedBytes(id);
        for (int i = 0; i < count; i++) {
            action.run();
        }
        final long after = bean.getThreadAllocatedBytes(id);
        if (before < 0 || after < before) {
            throw new AssertionError("allocation accounting unavailable");
        }
        return after - before;
    }

    private static ThreadMXBean allocationAccounting() {
        final java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assertThat(management).isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean)management;
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        if (!bean.isThreadAllocatedMemoryEnabled()) {
            bean.setThreadAllocatedMemoryEnabled(true);
        }
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        return bean;
    }
}
