package spine.examples.binance;

import org.agrona.DirectBuffer;
import org.agrona.collections.LongHashSet;

import java.nio.ByteOrder;

/** Owner-scoped scratch; outputs are usable only after a successful complete-frame decode. */
final class BinanceDepthDecoder {
    static final int MAX_LEVELS = 8192;
    static final int MALFORMED = 1, UNSUPPORTED = 2, OVERFLOW = 3, CAPACITY = 4, DUPLICATE = 5, SYMBOL = 6;
    private static final ByteOrder ORDER = ByteOrder.LITTLE_ENDIAN;
    final long[] bidPrices = new long[MAX_LEVELS], bidSizes = new long[MAX_LEVELS];
    final long[] askPrices = new long[MAX_LEVELS], askSizes = new long[MAX_LEVELS];
    int bidCount, askCount, error;
    long firstId, lastId;
    private final LongHashSet keys = new LongHashSet(MAX_LEVELS * 2, 0.6f);
    private DirectBuffer buffer;
    private int position, end, stringStart, stringLength;

    boolean decode(final DirectBuffer input, final int offset, final int length, final int format,
        final int priceScale, final int quantityScale) {
        bidCount = askCount = error = 0;
        firstId = lastId = 0;
        if (input == null || offset < 0 || length < 0 || offset > input.capacity() - length) {
            return fail(MALFORMED);
        }
        if (format < 1 || format > 3 || priceScale < 0 || priceScale > 18 || quantityScale < 0 || quantityScale > 18) {
            return fail(UNSUPPORTED);
        }
        buffer = input;
        position = offset;
        end = offset + length;
        final boolean valid = format == 2 ? sbe(priceScale, quantityScale) : json(format, priceScale, quantityScale);
        releaseKeys(true);
        releaseKeys(false);
        buffer = null;
        if (!valid) {
            bidCount = askCount = 0;
            firstId = lastId = 0;
        }
        return valid;
    }

    private boolean json(final int format, final int priceScale, final int quantityScale) {
        if (!take('{')) {
            return false;
        }
        int seen = 0;
        do {
            if (!plainString()) {
                return false;
            }
            final int key;
            if (matches("e")) { key = 1; }
            else if (matches("E")) { key = 2; }
            else if (matches("s")) { key = 4; }
            else if (matches("U")) { key = 8; }
            else if (matches("u") || matches("lastUpdateId")) { key = 16; }
            else if (matches("b") || matches("bids")) { key = 32; }
            else if (matches("a") || matches("asks")) { key = 64; }
            else { return fail(UNSUPPORTED); }
            // The two formats have distinct keys; aliases are not an extension of either wire contract.
            final boolean snapshotKey = matches("lastUpdateId") || matches("bids") || matches("asks");
            if ((format == 3) != snapshotKey || (seen & key) != 0) {
                return fail((seen & key) != 0 ? DUPLICATE : UNSUPPORTED);
            }
            seen |= key;
            if (!take(':')) {
                return false;
            }
            if (key == 1) {
                if (!plainString() || !matches("depthUpdate")) { return fail(UNSUPPORTED); }
            } else if (key == 4) {
                if (!plainString() || !matches("BTCUSDT")) { return fail(SYMBOL); }
            } else if (key == 32 || key == 64) {
                if (!jsonLevels(key == 32, priceScale, quantityScale)) { return false; }
            } else {
                final long value = unsignedInteger();
                if (error != 0) { return false; }
                if (key == 8) { firstId = value; }
                else if (key == 16) { lastId = value; }
            }
            whitespace();
            if (position < end && buffer.getByte(position) == '}') {
                position++;
                break;
            }
            if (!take(',')) {
                return false;
            }
        } while (true);
        whitespace();
        if (position != end || seen != (format == 3 ? 112 : 127)) {
            return fail(MALFORMED);
        }
        if (format == 3) { firstId = lastId; }
        return firstId <= lastId && (format == 3 || firstId > 0) || fail(MALFORMED);
    }

    private boolean jsonLevels(final boolean bids, final int priceScale, final int quantityScale) {
        if (!take('[')) { return false; }
        whitespace();
        if (position < end && buffer.getByte(position) == ']') {
            position++;
            return true;
        }
        do {
            if (!take('[')) { return false; }
            final long price = decimal(priceScale);
            if (error != 0 || !take(',')) { return false; }
            final long size = decimal(quantityScale);
            if (error != 0 || !take(']') || !level(bids, price, size)) { return false; }
            whitespace();
            if (position < end && buffer.getByte(position) == ']') {
                position++;
                releaseKeys(bids);
                return true;
            }
            if (!take(',')) { return false; }
        } while (true);
    }

    // Pinned official stream_1_0.xml: schema 1/version 0, DepthDiffStreamEvent 10003.
    // Root: eventTime, first/lastBookUpdateId, price/qty exponent; uint16 dimensions and 16-byte rows.
    private boolean sbe(final int priceScale, final int quantityScale) {
        if (end - position < 34) { return fail(MALFORMED); }
        if (u16(position) != 26 || u16(position + 2) != 10003 || u16(position + 4) != 1 ||
            u16(position + 6) != 0) { return fail(UNSUPPORTED); }
        if (buffer.getLong(position + 8, ORDER) < 0) { return fail(MALFORMED); }
        firstId = buffer.getLong(position + 16, ORDER);
        lastId = buffer.getLong(position + 24, ORDER);
        final int priceExponent = buffer.getByte(position + 32);
        final int quantityExponent = buffer.getByte(position + 33);
        if (firstId <= 0 || lastId < firstId) { return fail(MALFORMED); }
        if (priceExponent == -128 || quantityExponent == -128) { return fail(UNSUPPORTED); }
        position += 34;
        if (!sbeLevels(true, priceExponent + priceScale, quantityExponent + quantityScale) ||
            !sbeLevels(false, priceExponent + priceScale, quantityExponent + quantityScale)) { return false; }
        if (end - position != 8 || (buffer.getByte(position) & 255) != 7) { return fail(MALFORMED); }
        position++;
        stringStart = position;
        stringLength = 7;
        return matches("BTCUSDT") || fail(SYMBOL);
    }

    private boolean sbeLevels(final boolean bids, final int pricePower, final int quantityPower) {
        if (end - position < 4) { return fail(MALFORMED); }
        if (u16(position) != 16) { return fail(UNSUPPORTED); }
        final int count = u16(position + 2);
        position += 4;
        if (count > MAX_LEVELS) { return fail(CAPACITY); }
        if (count > (end - position) / 16) { return fail(MALFORMED); }
        for (int i = 0; i < count; i++) {
            final long price = scale(buffer.getLong(position, ORDER), pricePower);
            final long size = scale(buffer.getLong(position + 8, ORDER), quantityPower);
            if (error != 0 || !level(bids, price, size)) { return false; }
            position += 16;
        }
        releaseKeys(bids);
        return true;
    }

    private boolean level(final boolean bids, final long price, final long size) {
        if (price <= 0 || size < 0) { return fail(MALFORMED); }
        final int count = bids ? bidCount : askCount;
        if (count == MAX_LEVELS) { return fail(CAPACITY); }
        if (!keys.add(price)) { return fail(DUPLICATE); }
        if (bids) {
            bidPrices[count] = price;
            bidSizes[count] = size;
            bidCount++;
        } else {
            askPrices[count] = price;
            askSizes[count] = size;
            askCount++;
        }
        return true;
    }

    private void releaseKeys(final boolean bids) {
        final long[] prices = bids ? bidPrices : askPrices;
        final int count = bids ? bidCount : askCount;
        for (int i = 0; i < count; i++) { keys.remove(prices[i]); }
    }

    private long scale(final long mantissa, final int power) {
        if (mantissa < 0) {
            fail(MALFORMED);
            return 0;
        }
        if (mantissa == 0) { return 0; }
        if (power > 18 || power < -18) {
            fail(OVERFLOW);
            return 0;
        }
        long value = mantissa;
        for (int i = 0; i < power; i++) {
            if (value > Long.MAX_VALUE / 10) {
                fail(OVERFLOW);
                return 0;
            }
            value *= 10;
        }
        for (int i = 0; i > power; i--) {
            if (value % 10 != 0) {
                fail(OVERFLOW);
                return 0;
            }
            value /= 10;
        }
        return value;
    }

    private long decimal(final int scale) {
        if (!plainString()) { return 0; }
        final int start = stringStart, stop = start + stringLength;
        int cursor = start;
        while (cursor < stop && digit(buffer.getByte(cursor))) { cursor++; }
        final int whole = cursor - start;
        if (whole == 0) {
            fail(MALFORMED);
            return 0;
        }
        int fraction = 0;
        if (cursor < stop && buffer.getByte(cursor) == '.') {
            final int fractionStart = ++cursor;
            while (cursor < stop && digit(buffer.getByte(cursor))) { cursor++; }
            fraction = cursor - fractionStart;
            if (fraction == 0) {
                fail(MALFORMED);
                return 0;
            }
        }
        final int digitsEnd = cursor;
        int exponent = 0;
        if (cursor < stop && (buffer.getByte(cursor) == 'e' || buffer.getByte(cursor) == 'E')) {
            cursor++;
            boolean negative = false;
            if (cursor < stop && (buffer.getByte(cursor) == '-' || buffer.getByte(cursor) == '+')) {
                negative = buffer.getByte(cursor++) == '-';
            }
            final int exponentStart = cursor;
            while (cursor < stop && digit(buffer.getByte(cursor))) {
                if (exponent > 100_000) {
                    fail(OVERFLOW);
                    return 0;
                }
                exponent = exponent * 10 + buffer.getByte(cursor++) - '0';
            }
            if (cursor == exponentStart) {
                fail(MALFORMED);
                return 0;
            }
            if (negative) { exponent = -exponent; }
        }
        if (cursor != stop) {
            fail(MALFORMED);
            return 0;
        }
        final long boundary = (long)whole + scale + exponent;
        long value = 0;
        int index = 0;
        for (cursor = start; cursor < digitsEnd; cursor++) {
            final int ch = buffer.getByte(cursor);
            if (ch == '.') { continue; }
            final int digit = ch - '0';
            if (index++ >= boundary) {
                if (digit != 0) {
                    fail(OVERFLOW);
                    return 0;
                }
            } else {
                if (value > (Long.MAX_VALUE - digit) / 10) {
                    fail(OVERFLOW);
                    return 0;
                }
                value = value * 10 + digit;
            }
        }
        final long padding = boundary - whole - fraction;
        if (value != 0 && padding > 18) {
            fail(OVERFLOW);
            return 0;
        }
        for (long i = 0; value != 0 && i < padding; i++) {
            if (value > Long.MAX_VALUE / 10) {
                fail(OVERFLOW);
                return 0;
            }
            value *= 10;
        }
        return value;
    }

    private long unsignedInteger() {
        whitespace();
        final int start = position;
        long value = 0;
        while (position < end && digit(buffer.getByte(position))) {
            final int digit = buffer.getByte(position++) - '0';
            if (value > (Long.MAX_VALUE - digit) / 10) {
                fail(OVERFLOW);
                return 0;
            }
            value = value * 10 + digit;
        }
        if (position == start || position - start > 1 && buffer.getByte(start) == '0') { fail(MALFORMED); }
        return value;
    }

    private boolean plainString() {
        if (!take('"')) { return false; }
        stringStart = position;
        while (position < end && buffer.getByte(position) != '"') {
            final int ch = buffer.getByte(position++) & 255;
            if (ch < 32) { return fail(MALFORMED); }
            if (ch == '\\' || ch >= 128) { return fail(UNSUPPORTED); }
        }
        stringLength = position - stringStart;
        if (position == end) { return fail(MALFORMED); }
        position++;
        return true;
    }

    private boolean matches(final String expected) {
        if (stringLength != expected.length()) { return false; }
        for (int i = 0; i < stringLength; i++) {
            if (buffer.getByte(stringStart + i) != expected.charAt(i)) { return false; }
        }
        return true;
    }

    private boolean take(final int expected) {
        whitespace();
        if (position == end || buffer.getByte(position) != expected) { return fail(MALFORMED); }
        position++;
        return true;
    }

    private void whitespace() {
        while (position < end) {
            final byte ch = buffer.getByte(position);
            if (ch != ' ' && ch != '\t' && ch != '\n' && ch != '\r') { break; }
            position++;
        }
    }

    private int u16(final int offset) { return buffer.getShort(offset, ORDER) & 65535; }
    private static boolean digit(final int ch) { return ch >= '0' && ch <= '9'; }

    private boolean fail(final int value) {
        if (error == 0) {
            error = value;
        }
        return false;
    }
}
