package io.github.jstadeusiak.spine.examples.binance;

import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.sbe.ObservationProposalDecoder;
import io.github.jstadeusiak.spine.sequencer.Application;
import io.github.jstadeusiak.spine.protocol.ApplicationIdentity;
import io.github.jstadeusiak.spine.protocol.market.sbe.*;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.UnsafeBuffer;

/** One provisioned Binance Spot BTCUSDT book; all mutable storage belongs to the core owner. */
public final class BinanceApplication implements Application {
    public static final ApplicationIdentity IDENTITY = new ApplicationIdentity(401, 1, 1);
    static final int MAX_SOURCES = 16, MAX_BOOTSTRAP_BYTES = 512 * 1024;
    final BinanceBook book = new BinanceBook();
    private final BinanceDepthDecoder depth = new BinanceDepthDecoder();
    private final CaptureCodec capture = new CaptureCodec();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final BookSourceDecoder source = new BookSourceDecoder();
    private final BootstrapChunkDecoder chunk = new BootstrapChunkDecoder();
    private final BookResultEncoder result = new BookResultEncoder();
    private final BookResultDecoder cached = new BookResultDecoder();
    private final MarketSnapshotEncoder snapshot = new MarketSnapshotEncoder();
    private final int[] sources = new int[MAX_SOURCES], configurations = new int[MAX_SOURCES];
    private final int[] clocks = new int[MAX_SOURCES], formats = new int[MAX_SOURCES], order = new int[MAX_SOURCES];
    private final long[] sortedPrices = new long[BinanceBook.MAX_LEVELS];
    private final UnsafeBuffer bootstrap = new UnsafeBuffer(new byte[MAX_BOOTSTRAP_BYTES]);
    private int count, venueId, instrumentId, priceScale, quantityScale, outcome;
    private long epoch, maximumAgeNanos, bootstrapId;
    private int bootstrapSource, bootstrapConfig, bootstrapClock, bootstrapTotal, bootstrapFilled;

    @Override public int apply(final DirectBuffer input, final int offset, final int length,
        final ObservationProposalDecoder proposal, final long timeNanos,
        final MutableDirectBuffer output, final int outputOffset) {
        if (!applyObservation(input, offset, length, (int)proposal.sourceId(), (int)proposal.sourceConfigVersion(),
            (int)proposal.clockId(), timeNanos)) { return 0; }
        return encodeResult(output, outputOffset, timeNanos);
    }

    private boolean applyObservation(final DirectBuffer input, final int offset, final int length,
        final int sourceId, final int configuration, final int clockId, final long timeNanos) {
        outcome = 0;
        final int schema = input.getShort(offset + 4, Wire.ORDER) & 65535;
        final int template = input.getShort(offset + 2, Wire.ORDER) & 65535;
        if (schema == 401 && template == BookSourceDecoder.TEMPLATE_ID) {
            bind(input, offset, length, sourceId, configuration, clockId);
            return true;
        }
        final int binding = binding(sourceId, configuration, clockId);
        if (binding < 0) {
            for (int i = 0; i < count; i++) { if (sources[i] == sourceId) { invalidate(); return true; } }
            return false;
        }
        if (schema == 401 && template == BootstrapChunkDecoder.TEMPLATE_ID) {
            bootstrap(input, offset, length, binding, timeNanos);
        } else if (schema == 201 && template == 1) {
            // The typed binding defines interpretation; descriptive capture definitions remain audit material.
        } else if (capture.validObservation(input, offset, length)) {
            int rawOffset = capture.rawOffset, rawLength = capture.rawLength;
            if (formats[binding] == 2) {
                // The binary collector retains the application-message kind as a leading byte.
                if (rawLength == 0 || (input.getByte(rawOffset) != 0 && input.getByte(rawOffset) != 1)) {
                    invalidate(); return true;
                }
                final boolean text = input.getByte(rawOffset++) == 0;
                rawLength--;
                if (text) {
                    if (!control(input, rawOffset, rawLength)) { invalidate(); }
                    return true;
                }
            }
            if (formats[binding] == 2 || !control(input, rawOffset, rawLength)) {
                if (depth.decode(input, rawOffset, rawLength, formats[binding], priceScale, quantityScale)) {
                    outcome = book.apply(depth, formats[binding] == 3, timeNanos);
                } else { invalidate(); }
            }
        } else { invalidate(); }
        return true;
    }

    private void bind(final DirectBuffer input, final int offset, final int length,
        final int id, final int configuration, final int clock) {
        if (!fixed(input, offset, length, 1, BookSourceDecoder.BLOCK_LENGTH) || id == 0 || configuration == 0 || clock == 0) {
            invalidate(); return;
        }
        source.wrap(input, offset + 8, BookSourceDecoder.BLOCK_LENGTH, BookSourceDecoder.SCHEMA_VERSION);
        final long nextEpoch = source.epoch();
        final int nextVenue = (int)source.venueId(), nextInstrument = (int)source.instrumentId();
        if (nextVenue == 0 || nextVenue == -1 || nextInstrument == 0 || nextInstrument == -1 || nextEpoch <= 0
            || source.format() < 1 || source.format() > 3 || source.priceScale() > 18 || source.quantityScale() > 18
            || source.maximumAgeNanos() <= 0 || (epoch != 0 && (nextVenue != venueId || nextInstrument != instrumentId
                || nextEpoch < epoch || source.priceScale() != priceScale || source.quantityScale() != quantityScale
                || source.maximumAgeNanos() != maximumAgeNanos))) {
            invalidate(); return;
        }
        if (nextEpoch > epoch) {
            count = 0;
            book.bids.clear(); book.asks.clear();
            book.nativeFrontier = book.lastTimeNanos = book.bidFloor = book.askCeiling = 0;
            book.lastFirstId = book.lastLastId = 0;
            book.pendingCount = book.lastBidCount = book.lastAskCount = 0;
            book.status = BinanceBook.EMPTY;
            book.rebuildLastIndexes(); book.refreshBbo();
            clearBootstrap();
        }
        venueId = nextVenue; instrumentId = nextInstrument; epoch = nextEpoch;
        priceScale = source.priceScale(); quantityScale = source.quantityScale(); maximumAgeNanos = source.maximumAgeNanos();
        final int existing = binding(id, configuration, clock);
        if (existing >= 0) {
            if (formats[existing] != source.format()) { invalidate(); }
            return;
        }
        for (int i = 0; i < count; i++) {
            if (sources[i] == id && configurations[i] == configuration) { invalidate(); return; }
        }
        if (count == MAX_SOURCES) { book.status = BinanceBook.CAPACITY; outcome = 4; return; }
        sources[count] = id; configurations[count] = configuration; clocks[count] = clock; formats[count++] = source.format();
    }

    private int binding(final int id, final int configuration, final int clock) {
        for (int i = 0; i < count; i++) {
            if (sources[i] == id && configurations[i] == configuration && clocks[i] == clock) { return i; }
        }
        return -1;
    }

    private void bootstrap(final DirectBuffer input, final int offset, final int length,
        final int binding, final long timeNanos) {
        if (formats[binding] != 3 || length < 28 || !fixedHeader(input, offset, 2, BootstrapChunkDecoder.BLOCK_LENGTH)) {
            invalidate(); return;
        }
        chunk.wrap(input, offset + 8, BootstrapChunkDecoder.BLOCK_LENGTH, BootstrapChunkDecoder.SCHEMA_VERSION);
        final long id = chunk.bootstrapId(), total = chunk.totalLength(), start = chunk.chunkOffset();
        final int size = input.getInt(offset + 24, Wire.ORDER);
        if (id <= 0 || total <= 0 || total > MAX_BOOTSTRAP_BYTES || size <= 0 || size != length - 28
            || start > total - size) { invalidate(); clearBootstrap(); return; }
        if (start == 0) {
            if (bootstrapId != 0 && id <= bootstrapId) { invalidate(); return; }
            bootstrapId = id; bootstrapTotal = (int)total; bootstrapFilled = 0;
            bootstrapSource = sources[binding]; bootstrapConfig = configurations[binding]; bootstrapClock = clocks[binding];
        }
        if (id != bootstrapId || total != bootstrapTotal || start != bootstrapFilled
            || bootstrapSource != sources[binding] || bootstrapConfig != configurations[binding] || bootstrapClock != clocks[binding]) {
            invalidate(); clearBootstrap(); return;
        }
        bootstrap.putBytes(bootstrapFilled, input, offset + 28, size);
        bootstrapFilled += size;
        outcome = BinanceBook.NEEDS_BOOTSTRAP;
        if (bootstrapFilled == bootstrapTotal) {
            if (depth.decode(bootstrap, 0, bootstrapFilled, 3, priceScale, quantityScale)) {
                outcome = book.apply(depth, true, timeNanos);
            } else { invalidate(); }
            clearBootstrap();
        }
    }

    private void clearBootstrap() {
        bootstrapId = 0; bootstrapTotal = bootstrapFilled = bootstrapSource = bootstrapConfig = bootstrapClock = 0;
    }

    private void invalidate() { book.status = BinanceBook.INVALID; outcome = BinanceBook.INVALID_INPUT; }

    private static boolean fixedHeader(final DirectBuffer input, final int offset, final int template, final int block) {
        return (input.getShort(offset, Wire.ORDER) & 65535) == block
            && (input.getShort(offset + 2, Wire.ORDER) & 65535) == template
            && (input.getShort(offset + 4, Wire.ORDER) & 65535) == 401
            && input.getShort(offset + 6, Wire.ORDER) == BookSourceDecoder.SCHEMA_VERSION;
    }

    private static boolean fixed(final DirectBuffer input, final int offset, final int length, final int template, final int block) {
        return length == block + 8 && fixedHeader(input, offset, template, block);
    }

    private static boolean control(final DirectBuffer input, final int offset, final int length) {
        // Pinned direct subscriptions use this exact successful ACK; everything else needs its declared parser.
        final String ack = "{\"result\":null,\"id\":1}";
        if (length != ack.length()) { return false; }
        for (int i = 0; i < length; i++) {
            final int value = input.getByte(offset + i);
            if (i == length - 2 ? value != '1' && value != '2' : value != ack.charAt(i)) { return false; }
        }
        return true;
    }

    private int encodeResult(final MutableDirectBuffer output, final int offset, final long timeNanos) {
        final boolean eligible = book.eligible() && timeNanos >= book.lastTimeNanos
            && timeNanos - book.lastTimeNanos <= maximumAgeNanos;
        result.wrapAndApplyHeader(output, offset, headerEncoder).venueId(Integer.toUnsignedLong(venueId))
            .instrumentId(Integer.toUnsignedLong(instrumentId)).epoch(epoch).frontier(book.nativeFrontier)
            .lastAppliedTimeNanos(book.lastTimeNanos).bidFloor(book.bidFloor).askCeiling(book.askCeiling)
            .bidPrice(eligible ? book.bidPrice : 0).bidSize(eligible ? book.bidSize : 0)
            .askPrice(eligible ? book.askPrice : 0).askSize(eligible ? book.askSize : 0)
            .outcome((short)outcome).status((short)(book.status == BinanceBook.VALID && !eligible ? 6 : book.status))
            .priceScale((short)priceScale).quantityScale((short)quantityScale);
        return 8 + BookResultEncoder.BLOCK_LENGTH;
    }

    @Override public int snapshot(final MutableDirectBuffer output, final int offset) {
        final MarketSnapshotEncoder encoder = snapshot.wrapAndApplyHeader(output, offset, headerEncoder)
            .venueId(Integer.toUnsignedLong(venueId)).instrumentId(Integer.toUnsignedLong(instrumentId)).epoch(epoch)
            .priceScale((short)priceScale).quantityScale((short)quantityScale).maximumAgeNanos(maximumAgeNanos)
            .frontier(book.nativeFrontier).status((short)book.status).lastAppliedTimeNanos(book.lastTimeNanos)
            .bidFloor(book.bidFloor).askCeiling(book.askCeiling).lastFirstId(book.lastFirstId).lastLastId(book.lastLastId)
            .bootstrapId(bootstrapId).bootstrapSource(Integer.toUnsignedLong(bootstrapSource))
            .bootstrapConfig(Integer.toUnsignedLong(bootstrapConfig)).bootstrapClock(Integer.toUnsignedLong(bootstrapClock))
            .bootstrapTotal(bootstrapTotal);
        for (int i = 0; i < count; i++) {
            int j = i;
            while (j > 0 && compareBinding(order[j - 1], i) > 0) { order[j] = order[j - 1]; j--; }
            order[j] = i;
        }
        final MarketSnapshotEncoder.SourcesEncoder bindings = encoder.sourcesCount(count);
        for (int i = 0; i < count; i++) {
            final int index = order[i];
            bindings.next().sourceId(Integer.toUnsignedLong(sources[index])).configuration(Integer.toUnsignedLong(configurations[index]))
                .clockId(Integer.toUnsignedLong(clocks[index])).format((short)formats[index]);
        }
        final MarketSnapshotEncoder.LevelsEncoder levels = encoder.levelsCount(book.bids.size() + book.asks.size());
        for (int side = 0; side < 2; side++) {
            final Long2LongHashMap map = side == 0 ? book.bids : book.asks;
            int size = 0;
            final Long2LongHashMap.EntryIterator iterator = map.entrySet().iterator();
            while (iterator.hasNext()) { iterator.next(); sortedPrices[size++] = iterator.getLongKey(); }
            sortPrices(size);
            for (int i = 0; i < size; i++) { levels.next().side((short)side).price(sortedPrices[i]).size(map.get(sortedPrices[i])); }
        }
        final MarketSnapshotEncoder.LastLevelsEncoder last = encoder.lastLevelsCount(book.lastBidCount + book.lastAskCount);
        for (int i = 0; i < book.lastBidCount; i++) { last.next().side((short)0).price(book.lastBidPrices[i]).size(book.lastBidSizes[i]); }
        for (int i = 0; i < book.lastAskCount; i++) { last.next().side((short)1).price(book.lastAskPrices[i]).size(book.lastAskSizes[i]); }
        final MarketSnapshotEncoder.PendingEncoder pending = encoder.pendingCount(book.pendingCount);
        for (int i = 0; i < book.pendingCount; i++) {
            pending.next().firstId(book.pendingFirstId[i]).lastId(book.pendingLastId[i]).timeNanos(book.pendingTimeNanos[i]);
            final MarketSnapshotEncoder.PendingEncoder.LevelsEncoder rows = pending.levelsCount(book.pendingBidCount[i] + book.pendingAskCount[i]);
            for (int j = 0; j < book.pendingBidCount[i]; j++) { rows.next().side((short)0).price(book.pendingBidPrices[i][j]).size(book.pendingBidSizes[i][j]); }
            for (int j = 0; j < book.pendingAskCount[i]; j++) { rows.next().side((short)1).price(book.pendingAskPrices[i][j]).size(book.pendingAskSizes[i][j]); }
        }
        encoder.putBootstrapBytes(bootstrap, 0, bootstrapFilled);
        return 8 + encoder.encodedLength();
    }

    private int compareBinding(final int left, final int right) {
        int result = Integer.compareUnsigned(sources[left], sources[right]);
        if (result == 0) { result = Integer.compareUnsigned(configurations[left], configurations[right]); }
        if (result == 0) { result = Integer.compareUnsigned(clocks[left], clocks[right]); }
        return result;
    }

    // JDK primitive sorting may allocate merge workspace; snapshots use bounded in-place heapsort.
    private void sortPrices(final int size) {
        for (int root = size / 2 - 1; root >= 0; root--) { sift(root, size); }
        for (int end = size - 1; end > 0; end--) {
            final long top = sortedPrices[0]; sortedPrices[0] = sortedPrices[end]; sortedPrices[end] = top;
            sift(0, end);
        }
    }

    private void sift(final int start, final int size) {
        int root = start;
        final long value = sortedPrices[root];
        while (root < size / 2) {
            int child = root * 2 + 1;
            if (child + 1 < size && sortedPrices[child] < sortedPrices[child + 1]) { child++; }
            if (value >= sortedPrices[child]) { break; }
            sortedPrices[root] = sortedPrices[child]; root = child;
        }
        sortedPrices[root] = value;
    }

    @Override public Application restore(final DirectBuffer input, final int offset, final int length, final long sequence) {
        final BinanceApplication candidate = new BinanceApplication();
        candidate.readSnapshot(input, offset, length);
        require(sequence != 0 || candidate.pristine());
        return candidate;
    }

    @Override public void validateResult(final DirectBuffer input, final int offset, final int length) {
        if (length == 0) { return; }
        require(fixed(input, offset, length, BookResultDecoder.TEMPLATE_ID, BookResultDecoder.BLOCK_LENGTH));
        cached.wrap(input, offset + 8, BookResultDecoder.BLOCK_LENGTH, BookResultDecoder.SCHEMA_VERSION);
        final int status = cached.status(), outcome = cached.outcome();
        require(cached.venueId() != 0xFFFFFFFFL && cached.instrumentId() != 0xFFFFFFFFL
            && cached.epoch() >= 0 && cached.frontier() >= 0 && cached.lastAppliedTimeNanos() >= 0
            && cached.bidFloor() >= 0 && cached.askCeiling() >= 0 && cached.priceScale() <= 18 && cached.quantityScale() <= 18
            && status <= 6 && (outcome <= 4 || outcome == 6 || outcome == 8)
            && (cached.epoch() == 0 ? cached.venueId() == 0 && cached.instrumentId() == 0
                && cached.frontier() == 0 && status == BinanceBook.INVALID
                : cached.venueId() != 0 && cached.instrumentId() != 0)
            && (status == BinanceBook.VALID ? cached.frontier() > 0 && cached.bidFloor() > 0
                && cached.bidSize() > 0 && cached.askSize() > 0
                && cached.bidPrice() >= cached.bidFloor() && cached.bidPrice() > 0
                && cached.askPrice() <= cached.askCeiling() && cached.askPrice() > cached.bidPrice()
                : cached.bidPrice() == 0 && cached.bidSize() == 0 && cached.askPrice() == 0 && cached.askSize() == 0));
    }

    private void readSnapshot(final DirectBuffer input, final int offset, final int length) {
        require(Wire.range(input, offset, length) && length >= 127 && fixedHeader(input, offset, 4, MarketSnapshotDecoder.BLOCK_LENGTH));
        final MarketSnapshotDecoder decoder = new MarketSnapshotDecoder().wrap(input, offset + 8, MarketSnapshotDecoder.BLOCK_LENGTH, MarketSnapshotDecoder.SCHEMA_VERSION);
        venueId = (int)decoder.venueId(); instrumentId = (int)decoder.instrumentId(); epoch = decoder.epoch();
        priceScale = decoder.priceScale(); quantityScale = decoder.quantityScale(); maximumAgeNanos = decoder.maximumAgeNanos();
        book.nativeFrontier = decoder.frontier(); book.status = decoder.status(); book.lastTimeNanos = decoder.lastAppliedTimeNanos();
        book.bidFloor = decoder.bidFloor(); book.askCeiling = decoder.askCeiling();
        book.lastFirstId = decoder.lastFirstId(); book.lastLastId = decoder.lastLastId();
        bootstrapId = decoder.bootstrapId(); bootstrapSource = (int)decoder.bootstrapSource(); bootstrapConfig = (int)decoder.bootstrapConfig();
        bootstrapClock = (int)decoder.bootstrapClock(); bootstrapTotal = (int)decoder.bootstrapTotal();
        require(epoch >= 0 && venueId != -1 && instrumentId != -1 && priceScale <= 18 && quantityScale <= 18
            && book.nativeFrontier >= 0 && book.status <= BinanceBook.INVALID && book.lastTimeNanos >= 0
            && book.bidFloor >= 0 && book.askCeiling >= 0 && book.lastFirstId >= 0 && book.lastLastId >= book.lastFirstId
            && book.lastLastId <= book.nativeFrontier && bootstrapTotal >= 0 && bootstrapTotal <= MAX_BOOTSTRAP_BYTES);
        final MarketSnapshotDecoder.SourcesDecoder bindings = decoder.sources();
        require(bindings.count() <= MAX_SOURCES && bindings.actingBlockLength() == 13);
        for (int i = 0; i < bindings.count(); i++) {
            bindings.next(); sources[count] = (int)bindings.sourceId(); configurations[count] = (int)bindings.configuration();
            clocks[count] = (int)bindings.clockId(); formats[count] = bindings.format();
            require(sources[count] != 0 && sources[count] != -1 && configurations[count] != 0 && configurations[count] != -1
                && clocks[count] != 0 && clocks[count] != -1 && formats[count] >= 1 && formats[count] <= 3
                && (count == 0 || compareBinding(count - 1, count) < 0
                    && (sources[count - 1] != sources[count] || configurations[count - 1] != configurations[count])));
            count++;
        }
        final MarketSnapshotDecoder.LevelsDecoder levels = decoder.levels();
        require(levels.count() <= 2 * BinanceBook.MAX_LEVELS && levels.actingBlockLength() == 17);
        for (int i = 0; i < levels.count(); i++) {
            levels.next(); final int side = levels.side(); final long price = levels.price(), size = levels.size();
            require(side <= 1 && price > 0 && size > 0 && (side == 0 ? book.bidFloor > 0 && price >= book.bidFloor : price <= book.askCeiling));
            final Long2LongHashMap map = side == 0 ? book.bids : book.asks;
            require(map.size() < BinanceBook.MAX_LEVELS && map.put(price, size) == -1);
        }
        final MarketSnapshotDecoder.LastLevelsDecoder last = decoder.lastLevels();
        require(last.count() <= 2 * BinanceBook.MAX_LEVELS && last.actingBlockLength() == 17);
        for (int i = 0; i < last.count(); i++) {
            last.next(); final int side = last.side(); final long price = last.price(), size = last.size();
            require(side <= 1 && price > 0 && size >= 0);
            final int row = side == 0 ? book.lastBidCount++ : book.lastAskCount++;
            require(row < BinanceBook.MAX_LEVELS);
            (side == 0 ? book.lastBidPrices : book.lastAskPrices)[row] = price;
            (side == 0 ? book.lastBidSizes : book.lastAskSizes)[row] = size;
        }
        final MarketSnapshotDecoder.PendingDecoder pending = decoder.pending();
        final LongHashSet keys = new LongHashSet(BinanceBook.MAX_PENDING_LEVELS * 2);
        require(pending.count() <= BinanceBook.MAX_PENDING && pending.actingBlockLength() == 24);
        for (int i = 0; i < pending.count(); i++) {
            pending.next(); book.pendingFirstId[i] = pending.firstId(); book.pendingLastId[i] = pending.lastId(); book.pendingTimeNanos[i] = pending.timeNanos();
            require(book.pendingFirstId[i] > 0 && book.pendingLastId[i] >= book.pendingFirstId[i] && book.pendingTimeNanos[i] >= 0);
            for (int earlier = 0; earlier < i; earlier++) {
                require(book.pendingFirstId[i] != book.pendingFirstId[earlier] || book.pendingLastId[i] != book.pendingLastId[earlier]);
            }
            final MarketSnapshotDecoder.PendingDecoder.LevelsDecoder rows = pending.levels();
            require(rows.count() <= 2 * BinanceBook.MAX_PENDING_LEVELS && rows.actingBlockLength() == 17);
            for (int j = 0; j < rows.count(); j++) {
                rows.next(); final int side = rows.side(); final long price = rows.price(), size = rows.size();
                require(side <= 1 && price > 0 && size >= 0);
                final int row = side == 0 ? book.pendingBidCount[i]++ : book.pendingAskCount[i]++;
                require(row < BinanceBook.MAX_PENDING_LEVELS);
                (side == 0 ? book.pendingBidPrices : book.pendingAskPrices)[i][row] = price;
                (side == 0 ? book.pendingBidSizes : book.pendingAskSizes)[i][row] = size;
            }
            for (int side = 0; side < 2; side++) {
                keys.clear();
                final long[] prices = side == 0 ? book.pendingBidPrices[i] : book.pendingAskPrices[i];
                final int rowsCount = side == 0 ? book.pendingBidCount[i] : book.pendingAskCount[i];
                for (int row = 0; row < rowsCount; row++) { require(keys.add(prices[row])); }
            }
            book.pendingCount++;
        }
        bootstrapFilled = decoder.bootstrapBytesLength();
        require(bootstrapFilled >= 0 && bootstrapFilled <= bootstrapTotal && bootstrapFilled <= MAX_BOOTSTRAP_BYTES
            && decoder.limit() + 4 + bootstrapFilled == offset + length);
        decoder.getBootstrapBytes(bootstrap, 0, bootstrapFilled);
        require((epoch == 0 ? count == 0 && venueId == 0 && instrumentId == 0 && book.nativeFrontier == 0
            && (book.status == BinanceBook.EMPTY || book.status == BinanceBook.INVALID)
            && book.bids.isEmpty() && book.asks.isEmpty() && bootstrapTotal == 0
            : count > 0 && venueId != 0 && instrumentId != 0 && maximumAgeNanos > 0)
            && (bootstrapTotal == 0 ? bootstrapId == 0 && bootstrapFilled == 0
                && bootstrapSource == 0 && bootstrapConfig == 0 && bootstrapClock == 0 : bootstrapId > 0
                && bootstrapFilled > 0 && bootstrapFilled < bootstrapTotal
                && binding(bootstrapSource, bootstrapConfig, bootstrapClock) >= 0
                && formats[binding(bootstrapSource, bootstrapConfig, bootstrapClock)] == 3));
        book.rebuildLastIndexes(); book.refreshBbo();
        require((book.lastFirstId == 0 ? book.lastLastId == 0 && book.lastBidCount == 0 && book.lastAskCount == 0
                : book.lastLastId == book.nativeFrontier)
            && (book.status != BinanceBook.VALID || book.nativeFrontier > 0 && book.pendingCount == 0
                && (book.bidPrice == 0 || book.askPrice == 0 || book.bidPrice < book.askPrice))
            && (book.status != BinanceBook.EMPTY || book.nativeFrontier == 0 && book.bids.isEmpty() && book.asks.isEmpty()
                && book.bidFloor == 0 && book.askCeiling == 0 && book.lastLastId == 0 && book.pendingCount == 0));
        if (book.status == BinanceBook.GAP) {
            require(book.pendingCount > 0 && book.nativeFrontier > 0 && book.nativeFrontier < Long.MAX_VALUE);
            for (int i = 0; i < book.pendingCount; i++) { require(book.pendingFirstId[i] > book.nativeFrontier + 1); }
        }
    }

    private static void require(final boolean valid) {
        if (!valid) { throw new IllegalArgumentException("invalid market snapshot"); }
    }

    private boolean pristine() {
        return epoch == 0 && book.status == BinanceBook.EMPTY && priceScale == 0 && quantityScale == 0
            && maximumAgeNanos == 0 && book.lastTimeNanos == 0 && book.pendingCount == 0;
    }
}
