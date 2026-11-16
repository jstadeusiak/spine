package io.github.jstadeusiak.spine.examples.binance;

import org.agrona.collections.Hashing;
import org.agrona.collections.Long2LongHashMap;

/** One owner. Absolute quantities, bounded bootstrap coverage and native-range continuity. */
final class BinanceBook {
    static final int MAX_LEVELS = 8_192;
    static final int MAX_PENDING = 8;
    static final int MAX_PENDING_LEVELS = 512;
    static final int EMPTY = 0, VALID = 1, GAP = 2, CONFLICT = 3, CAPACITY = 4, INVALID = 5;
    static final int APPLIED = 1, STALE = 2, PENDING = 3, NEEDS_BOOTSTRAP = 4, INVALID_INPUT = 6,
        CONFLICTING = 8;

    final Long2LongHashMap bids = map();
    final Long2LongHashMap asks = map();
    private final Long2LongHashMap lastBidIndexes = map();
    private final Long2LongHashMap lastAskIndexes = map();
    long nativeFrontier, lastTimeNanos, bidFloor, askCeiling, bidPrice, bidSize, askPrice, askSize;
    int status;
    long lastFirstId, lastLastId;
    int lastBidCount, lastAskCount;
    final long[] lastBidPrices = new long[MAX_LEVELS];
    final long[] lastBidSizes = new long[MAX_LEVELS];
    final long[] lastAskPrices = new long[MAX_LEVELS];
    final long[] lastAskSizes = new long[MAX_LEVELS];
    int pendingCount;
    final long[] pendingFirstId = new long[MAX_PENDING];
    final long[] pendingLastId = new long[MAX_PENDING];
    final long[] pendingTimeNanos = new long[MAX_PENDING];
    final int[] pendingBidCount = new int[MAX_PENDING];
    final int[] pendingAskCount = new int[MAX_PENDING];
    final long[][] pendingBidPrices = new long[MAX_PENDING][MAX_PENDING_LEVELS];
    final long[][] pendingBidSizes = new long[MAX_PENDING][MAX_PENDING_LEVELS];
    final long[][] pendingAskPrices = new long[MAX_PENDING][MAX_PENDING_LEVELS];
    final long[][] pendingAskSizes = new long[MAX_PENDING][MAX_PENDING_LEVELS];

    private static Long2LongHashMap map() {
        final Long2LongHashMap map = new Long2LongHashMap(MAX_LEVELS * 2, Hashing.DEFAULT_LOAD_FACTOR, -1L);
        map.entrySet().iterator(); // Initialize the borrowed iterator before readiness.
        return map;
    }

    int apply(final BinanceDepthDecoder update, final boolean snapshot, final long timeNanos) {
        if (update.firstId <= 0 || update.lastId < update.firstId || timeNanos < 0
            || !validRows(update.bidPrices, update.bidSizes, update.bidCount, snapshot)
            || !validRows(update.askPrices, update.askSizes, update.askCount, snapshot)) {
            status = INVALID;
            return INVALID_INPUT;
        }
        if (snapshot) {
            if (update.lastId < nativeFrontier) {
                return STALE;
            }
            long bestBid = 0, bestAsk = Long.MAX_VALUE;
            long floor = Long.MAX_VALUE, ceiling = 0;
            for (int i = 0; i < update.bidCount; i++) {
                bestBid = Math.max(bestBid, update.bidPrices[i]);
                floor = Math.min(floor, update.bidPrices[i]);
            }
            for (int i = 0; i < update.askCount; i++) {
                bestAsk = Math.min(bestAsk, update.askPrices[i]);
                ceiling = Math.max(ceiling, update.askPrices[i]);
            }
            if (update.bidCount > 0 && update.askCount > 0 && bestBid >= bestAsk) {
                status = INVALID;
                return INVALID_INPUT;
            }
            bids.clear();
            asks.clear();
            bidFloor = update.bidCount == 0 ? 0 : floor;
            askCeiling = ceiling;
            mutate(bids, update.bidPrices, update.bidSizes, update.bidCount, true);
            mutate(asks, update.askPrices, update.askSizes, update.askCount, false);
            nativeFrontier = update.lastId;
            lastTimeNanos = timeNanos;
            pendingCount = lastBidCount = lastAskCount = 0;
            lastFirstId = lastLastId = 0;
            rebuildLastIndexes();
            status = VALID;
            refreshBbo();
            return APPLIED;
        }
        if (status != VALID && status != GAP) {
            return NEEDS_BOOTSTRAP;
        }
        if (update.lastId <= nativeFrontier) {
            return checkStale(update.firstId, update.lastId, update.bidPrices, update.bidSizes, update.bidCount,
                update.askPrices, update.askSizes, update.askCount);
        }
        if (update.firstId > nativeFrontier + 1) {
            for (int i = 0; i < pendingCount; i++) {
                if (pendingFirstId[i] == update.firstId && pendingLastId[i] == update.lastId) {
                    if (sameRows(pendingBidPrices[i], pendingBidSizes[i], pendingBidCount[i],
                            update.bidPrices, update.bidSizes, update.bidCount)
                        && sameRows(pendingAskPrices[i], pendingAskSizes[i], pendingAskCount[i],
                            update.askPrices, update.askSizes, update.askCount)) {
                        return PENDING;
                    }
                    status = CONFLICT;
                    return CONFLICTING;
                }
            }
            if (pendingCount == MAX_PENDING || update.bidCount > MAX_PENDING_LEVELS
                || update.askCount > MAX_PENDING_LEVELS) {
                status = CAPACITY;
                return NEEDS_BOOTSTRAP;
            }
            final int slot = pendingCount++;
            pendingFirstId[slot] = update.firstId;
            pendingLastId[slot] = update.lastId;
            pendingTimeNanos[slot] = timeNanos;
            pendingBidCount[slot] = update.bidCount;
            pendingAskCount[slot] = update.askCount;
            copy(update.bidPrices, update.bidSizes, pendingBidPrices[slot], pendingBidSizes[slot], update.bidCount);
            copy(update.askPrices, update.askSizes, pendingAskPrices[slot], pendingAskSizes[slot], update.askCount);
            status = GAP;
            return PENDING;
        }
        int outcome = advance(update.firstId, update.lastId, update.bidPrices, update.bidSizes, update.bidCount,
            update.askPrices, update.askSizes, update.askCount, timeNanos);
        if (outcome != APPLIED) {
            return outcome;
        }
        // Scan in committed arrival order; advancing a frontier can qualify an earlier pending range.
        for (int i = 0; i < pendingCount;) {
            if (pendingLastId[i] <= nativeFrontier) {
                outcome = checkStale(pendingFirstId[i], pendingLastId[i], pendingBidPrices[i], pendingBidSizes[i],
                    pendingBidCount[i], pendingAskPrices[i], pendingAskSizes[i], pendingAskCount[i]);
            } else if (pendingFirstId[i] <= nativeFrontier + 1) {
                outcome = advance(pendingFirstId[i], pendingLastId[i], pendingBidPrices[i], pendingBidSizes[i],
                    pendingBidCount[i], pendingAskPrices[i], pendingAskSizes[i], pendingAskCount[i], pendingTimeNanos[i]);
            } else {
                i++;
                continue;
            }
            if (outcome != APPLIED && outcome != STALE) {
                return outcome;
            }
            removePending(i);
            i = 0;
        }
        status = pendingCount == 0 ? VALID : GAP;
        return APPLIED;
    }

    private static boolean validRows(final long[] prices, final long[] sizes, final int count,
        final boolean snapshot) {
        if (count < 0 || count > MAX_LEVELS) {
            return false;
        }
        for (int i = 0; i < count; i++) {
            if (prices[i] <= 0 || sizes[i] < 0 || (snapshot && sizes[i] == 0)) {
                return false;
            }
        }
        return true;
    }

    private int advance(final long first, final long last, final long[] bidPrices, final long[] bidSizes,
        final int bidCount, final long[] askPrices, final long[] askSizes, final int askCount, final long timeNanos) {
        if (!fits(bids, bidPrices, bidSizes, bidCount, true) || !fits(asks, askPrices, askSizes, askCount, false)) {
            status = CAPACITY;
            return NEEDS_BOOTSTRAP;
        }
        mutate(bids, bidPrices, bidSizes, bidCount, true);
        mutate(asks, askPrices, askSizes, askCount, false);
        nativeFrontier = last;
        lastTimeNanos = timeNanos;
        lastFirstId = first;
        lastLastId = last;
        retain(lastBidIndexes, lastBidPrices, lastBidSizes, lastBidCount, bidPrices, bidSizes, bidCount);
        retain(lastAskIndexes, lastAskPrices, lastAskSizes, lastAskCount, askPrices, askSizes, askCount);
        lastBidCount = bidCount;
        lastAskCount = askCount;
        refreshSide(bids, bidPrices, bidSizes, bidCount, true);
        refreshSide(asks, askPrices, askSizes, askCount, false);
        if (bidPrice > 0 && askPrice > 0 && bidPrice >= askPrice) {
            status = INVALID;
            return INVALID_INPUT;
        }
        return APPLIED;
    }

    private boolean covered(final long price, final boolean bid) {
        return bid ? bidFloor > 0 && price >= bidFloor : price <= askCeiling;
    }

    private boolean fits(final Long2LongHashMap levels, final long[] prices, final long[] sizes,
        final int count, final boolean bid) {
        int resultingSize = levels.size();
        for (int i = 0; i < count; i++) {
            if (covered(prices[i], bid)) {
                final boolean exists = levels.get(prices[i]) != -1;
                if (sizes[i] == 0 && exists) {
                    resultingSize--;
                } else if (sizes[i] != 0 && !exists) {
                    resultingSize++;
                }
            }
        }
        return resultingSize <= MAX_LEVELS;
    }

    private void mutate(final Long2LongHashMap levels, final long[] prices, final long[] sizes,
        final int count, final boolean bid) {
        for (int i = 0; i < count; i++) {
            if (sizes[i] == 0 && covered(prices[i], bid)) {
                levels.remove(prices[i]);
            }
        }
        for (int i = 0; i < count; i++) {
            if (sizes[i] > 0 && covered(prices[i], bid)) {
                levels.put(prices[i], sizes[i]);
            }
        }
    }

    private int checkStale(final long first, final long last, final long[] bidPrices, final long[] bidSizes,
        final int bidCount, final long[] askPrices, final long[] askSizes, final int askCount) {
        final boolean exactSpanConflict = first == lastFirstId && last == lastLastId
            && (bidCount != lastBidCount || askCount != lastAskCount
                || !matches(lastBidIndexes, lastBidSizes, bidPrices, bidSizes, bidCount)
                || !matches(lastAskIndexes, lastAskSizes, askPrices, askSizes, askCount));
        if (exactSpanConflict || last == nativeFrontier
            && (!matchesBook(bids, bidPrices, bidSizes, bidCount, true)
                || !matchesBook(asks, askPrices, askSizes, askCount, false))) {
            status = CONFLICT;
            return CONFLICTING;
        }
        return STALE;
    }

    private boolean matchesBook(final Long2LongHashMap levels, final long[] prices, final long[] sizes,
        final int count, final boolean bid) {
        for (int i = 0; i < count; i++) {
            if (covered(prices[i], bid) && levels.get(prices[i]) != (sizes[i] == 0 ? -1 : sizes[i])) {
                return false;
            }
        }
        return true;
    }

    private static boolean sameRows(final long[] leftPrices, final long[] leftSizes, final int leftCount,
        final long[] rightPrices, final long[] rightSizes, final int rightCount) {
        if (leftCount != rightCount) {
            return false;
        }
        for (int i = 0; i < leftCount; i++) {
            int j = 0;
            while (j < rightCount && leftPrices[i] != rightPrices[j]) {
                j++;
            }
            if (j == rightCount || leftSizes[i] != rightSizes[j]) {
                return false;
            }
        }
        return true;
    }

    private static boolean matches(final Long2LongHashMap indexes, final long[] retainedSizes,
        final long[] prices, final long[] sizes, final int count) {
        for (int i = 0; i < count; i++) {
            final long index = indexes.get(prices[i]);
            if (index < 0 || retainedSizes[(int)index] != sizes[i]) {
                return false;
            }
        }
        return true;
    }

    private static void retain(final Long2LongHashMap indexes, final long[] retainedPrices,
        final long[] retainedSizes, final int oldCount, final long[] prices, final long[] sizes, final int count) {
        for (int i = 0; i < oldCount; i++) {
            indexes.remove(retainedPrices[i]);
        }
        copy(prices, sizes, retainedPrices, retainedSizes, count);
        for (int i = 0; i < count; i++) {
            indexes.put(prices[i], i);
        }
    }

    void rebuildLastIndexes() {
        lastBidIndexes.clear();
        lastAskIndexes.clear();
        for (int i = 0; i < lastBidCount; i++) {
            if (lastBidIndexes.put(lastBidPrices[i], i) != -1) {
                throw new IllegalArgumentException("duplicate snapshot last bid");
            }
        }
        for (int i = 0; i < lastAskCount; i++) {
            if (lastAskIndexes.put(lastAskPrices[i], i) != -1) {
                throw new IllegalArgumentException("duplicate snapshot last ask");
            }
        }
        if (!matchesBook(bids, lastBidPrices, lastBidSizes, lastBidCount, true)
            || !matchesBook(asks, lastAskPrices, lastAskSizes, lastAskCount, false)) {
            throw new IllegalArgumentException("snapshot last rows differ from current book");
        }
    }

    void refreshBbo() {
        bidPrice = bidSize = askPrice = askSize = 0;
        refreshSide(bids, lastBidPrices, lastBidSizes, 0, true);
        refreshSide(asks, lastAskPrices, lastAskSizes, 0, false);
    }

    private void refreshSide(final Long2LongHashMap levels, final long[] prices, final long[] sizes,
        final int count, final boolean bid) {
        long best = bid ? bidPrice : askPrice;
        long size = levels.get(best);
        if (best == 0 || size == -1) {
            best = size = 0;
            final Long2LongHashMap.EntryIterator iterator = levels.entrySet().iterator();
            while (iterator.hasNext()) {
                iterator.next();
                final long price = iterator.getLongKey();
                if (best == 0 || (bid ? price > best : price < best)) {
                    best = price;
                    size = iterator.getLongValue();
                }
            }
        } else {
            for (int i = 0; i < count; i++) {
                if (sizes[i] > 0 && covered(prices[i], bid) && (bid ? prices[i] > best : prices[i] < best)) {
                    best = prices[i];
                    size = sizes[i];
                }
            }
        }
        if (bid) {
            bidPrice = best; bidSize = size;
        } else {
            askPrice = best; askSize = size;
        }
    }

    boolean eligible() {
        return status == VALID && bidPrice > 0 && askPrice > bidPrice
            && covered(bidPrice, true) && covered(askPrice, false);
    }

    private static void copy(final long[] prices, final long[] sizes, final long[] targetPrices,
        final long[] targetSizes, final int count) {
        System.arraycopy(prices, 0, targetPrices, 0, count);
        System.arraycopy(sizes, 0, targetSizes, 0, count);
    }

    private void removePending(final int slot) {
        pendingCount--;
        for (int i = slot; i < pendingCount; i++) {
            pendingFirstId[i] = pendingFirstId[i + 1];
            pendingLastId[i] = pendingLastId[i + 1];
            pendingTimeNanos[i] = pendingTimeNanos[i + 1];
            pendingBidCount[i] = pendingBidCount[i + 1];
            pendingAskCount[i] = pendingAskCount[i + 1];
            copy(pendingBidPrices[i + 1], pendingBidSizes[i + 1], pendingBidPrices[i], pendingBidSizes[i],
                pendingBidCount[i]);
            copy(pendingAskPrices[i + 1], pendingAskSizes[i + 1], pendingAskPrices[i], pendingAskSizes[i],
                pendingAskCount[i]);
        }
    }
}
