package spine.examples.binance;

import com.sun.management.ThreadMXBean;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class BinanceDepthDecoderTest {
    // Independent fixed bytes: official schema1/v0 DepthDiffStreamEvent; two bids, one ask.
    private static final String SBE_HEX = "1a0013270100000000401e18240a0600f401000000000000f901000000000000fefd" +
        "100002001b87010000000000d711000000000000a0860100000000000000000000000000" +
        "10000100688801000000000019000000000000000742544355534454";
    private static final String JSON = "{\"e\":\"depthUpdate\",\"E\":1700000000000000,\"s\":\"BTCUSDT\",\"U\":500," +
        "\"u\":505,\"b\":[[\"1001.23\",\"4.567\"],[\"1000.00\",\"0\"]],\"a\":[[\"1004.56\",\"0.025\"]]}";
    private static volatile byte[] escaping;
    private static volatile long consumed;
    private final BinanceDepthDecoder decoder = new BinanceDepthDecoder();

    @Test
    void fixedOfficialLayoutAndJsonHaveIdenticalExactLevelsIncludingDeletion() {
        assertThat(decode(JSON, 1, 2, 3)).isTrue();
        assertFixture();
        final UnsafeBuffer binary = sbe();
        assertThat(decoder.decode(binary, 0, binary.capacity(), 2, 2, 3)).isTrue();
        assertFixture();
        assertThat(decoder.decode(binary, 0, binary.capacity(), 2, 5, 8)).isTrue();
        assertThat(decoder.bidPrices[0]).isEqualTo(100_123_000L);
        assertThat(decoder.bidSizes[0]).isEqualTo(456_700_000L);
        assertThat(decoder.askPrices[0]).isEqualTo(100_456_000L);
        assertThat(decoder.askSizes[0]).isEqualTo(2_500_000L);
    }

    @Test
    void snapshotIsItsOwnStrictFormatAndAllowsEmptySidesAndZeroUpdateId() {
        assertThat(decode(" { \"asks\" : [], \"lastUpdateId\" : 0, \"bids\" : [] } \r\n", 3, 2, 3)).isTrue();
        assertThat(decoder.firstId).isZero();
        assertThat(decoder.lastId).isZero();
        assertThat(decoder.bidCount).isZero();
        assertThat(decoder.askCount).isZero();
        assertThat(decode("{\"lastUpdateId\":9223372036854775807,\"bids\":[[\"1e3\",\"0\"]],\"asks\":[]}",
            3, 2, 3)).isTrue();
        assertThat(decoder.firstId).isEqualTo(Long.MAX_VALUE);
        assertThat(decoder.lastId).isEqualTo(Long.MAX_VALUE);
        assertThat(decoder.bidPrices[0]).isEqualTo(100_000);
        assertThat(decoder.bidSizes[0]).isZero();
        assertThat(decode(JSON, 3, 2, 3)).isFalse();
        assertThat(decode("{\"lastUpdateId\":1,\"bids\":[],\"asks\":[]}", 1, 2, 3)).isFalse();
    }

    @Test
    void checkedDecimalNormalizationAgreesWithAnIndependentBigDecimalOracle() {
        final Random random = new Random(7831);
        for (int i = 0; i < 2000; i++) {
            final int scale = random.nextInt(19);
            final long price = 1 + random.nextLong(1_000_000_000_000L);
            final long quantity = random.nextLong(1_000_000_000_000L);
            final BigDecimal exactPrice = BigDecimal.valueOf(price, scale).setScale(scale + random.nextInt(24));
            final BigDecimal exactSize = BigDecimal.valueOf(quantity, scale).setScale(scale + random.nextInt(24));
            final String priceText = i % 2 == 0 ? exactPrice.toEngineeringString() : exactPrice.toPlainString();
            final String sizeText = i % 2 == 0 ? exactSize.toEngineeringString() : exactSize.toPlainString();
            assertThat(decode(snapshot(priceText, sizeText), 3, scale, scale)).as("%s / %s", priceText, sizeText).isTrue();
            assertThat(decoder.bidPrices[0]).isEqualTo(new BigDecimal(priceText).movePointRight(scale).longValueExact());
            assertThat(decoder.bidSizes[0]).isEqualTo(new BigDecimal(sizeText).movePointRight(scale).longValueExact());
        }
        assertThat(decode(snapshot("9223372036854775807", "000.000e-9999"), 3, 0, 18)).isTrue();
        assertThat(decoder.bidPrices[0]).isEqualTo(Long.MAX_VALUE);
        assertThat(decoder.bidSizes[0]).isZero();
        assertThat(decode(snapshot("00000000000000000001.000000000000000000000", "0.01000e+2"), 3, 0, 0)).isTrue();
        assertThat(decoder.bidPrices[0]).isOne();
        assertThat(decoder.bidSizes[0]).isOne();
    }

    @Test
    void malformedUnsupportedAndOverflowFramesCannotExposePartialScratchAsValid() {
        final String[] malformed = {
            "", "{}", JSON + "x", JSON + "{}", JSON.replace("1700000000000000", "01"),
            JSON.replace("\"U\":500", "\"U\":0"), JSON.replace("\"u\":505", "\"u\":499"),
            JSON.replace("\"E\":1700000000000000,", ""), JSON.replace("\"U\":500", "\"U\":\"500\""),
            JSON.replace("\"1001.23\"", "\"-1001.23\""), JSON.replace("\"4.567\"", "\"-4.567\""),
            JSON.replace("\"1001.23\"", "\"0\""), JSON.replace("\"4.567\"", "\"NaN\""),
            JSON.replace("\"4.567\"", "4.567"), JSON.replace("\"4.567\"", "\".5\""),
            JSON.replace("\"4.567\"", "\"4.\""), JSON.replace("\"4.567\"", "\"1e\""),
            JSON.replace("\"4.567\"", "\"1e+\""), JSON.replace("\"4.567\"", "\"1.2.3\""),
            JSON.replace("\"4.567\"]", "\"4.567\",\"2\"]"), JSON.replace("\"4.567\"", "\"4\n5\""),
            JSON.replace("\"E\":1700000000000000", "\"E\":1700000000000000e0"),
            JSON.replace("\"e\":\"depthUpdate\"", "\"e\":\"trade\""),
            JSON.replace("\"s\":\"BTCUSDT\"", "\"s\":\"ETHUSDT\""),
            JSON.replace("\"s\":", "\"\\u0073\":"), JSON.replace("\"BTCUSDT\"", "\"BTCUSD\\u0054\""),
            JSON.replace("\"s\":", "\"µ\":"), JSON.replace("\"E\":1700000000000000", "\"unknown\":{}")
        };
        for (final String frame : malformed) {
            assertThat(decode(frame, 1, 2, 3)).as(frame).isFalse();
            assertRejectedScratch();
            assertThat(decode(JSON, 1, 2, 3)).isTrue();
            assertFixture();
        }
        for (final String text : new String[] {"9223372036854775808", "1e19", "0.000000001", "1e-9999", "1e9999999"}) {
            assertThat(decode(snapshot(text, "1"), 3, 0, 0)).as(text).isFalse();
            assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.OVERFLOW);
        }
        assertThat(decode(JSON.replace("\"u\":505", "\"u\":9223372036854775808"), 1, 2, 3)).isFalse();
        assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.OVERFLOW);
    }

    @Test
    void duplicateFieldsAndEquivalentPriceKeysAreRejectedWithoutPoisoningTheNextDecode() {
        for (final String field : new String[] {"\"e\":\"depthUpdate\"", "\"E\":1", "\"s\":\"BTCUSDT\"",
            "\"U\":500", "\"u\":505", "\"b\":[]", "\"a\":[]"}) {
            assertThat(decode(JSON.substring(0, JSON.length() - 1) + "," + field + "}", 1, 2, 3)).isFalse();
            assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.DUPLICATE);
        }
        final String repeated = JSON.replace("[\"1000.00\",\"0\"]", "[\"1001.2300\",\"0\"]");
        assertThat(decode(repeated, 1, 2, 3)).isFalse();
        assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.DUPLICATE);
        final UnsafeBuffer binary = sbe();
        binary.putLong(54, 100123, ByteOrder.LITTLE_ENDIAN);
        assertThat(decoder.decode(binary, 0, binary.capacity(), 2, 2, 3)).isFalse();
        assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.DUPLICATE);
        assertThat(decode(JSON, 1, 2, 3)).isTrue();
        assertFixture();
        // Identical prices on opposite sides are separate keys; crossed-book validation belongs to the book.
        assertThat(decode("{\"lastUpdateId\":1,\"bids\":[[\"1\",\"2\"]],\"asks\":[[\"1.00\",\"3\"]]}", 3, 2, 0)).isTrue();
    }

    @Test
    void allTruncationsAndInvalidBufferRangesFailBeforeOutOfBoundsReads() {
        for (final int format : new int[] {1, 2, 3}) {
            final UnsafeBuffer frame = format == 2 ? sbe() : utf8(format == 1 ? JSON : snapshot("1", "2"));
            for (int length = 0; length < frame.capacity(); length++) {
                assertThat(decoder.decode(frame, 0, length, format, 2, 3)).as("format %s length %s", format, length).isFalse();
                assertRejectedScratch();
            }
            final UnsafeBuffer offset = new UnsafeBuffer(new byte[frame.capacity() + 17]);
            offset.putBytes(9, frame, 0, frame.capacity());
            assertThat(decoder.decode(offset, 9, frame.capacity(), format, 2, 3)).isTrue();
            assertThat(decoder.decode(frame, -1, frame.capacity(), format, 2, 3)).isFalse();
            assertThat(decoder.decode(frame, 1, Integer.MAX_VALUE, format, 2, 3)).isFalse();
            assertThat(decoder.decode(frame, 0, -1, format, 2, 3)).isFalse();
        }
        assertThat(decoder.decode(null, 0, 0, 1, 2, 3)).isFalse();
        assertThat(decode(JSON, 0, 2, 3)).isFalse();
        assertThat(decode(JSON, 1, -1, 3)).isFalse();
        assertThat(decode(JSON, 1, 2, 19)).isFalse();
    }

    @Test
    void everySbeBoundaryIsCheckedIncludingSchemaNullsDimensionsSymbolAndExactScaling() {
        for (final int offset : new int[] {0, 2, 4, 6, 34, 70}) {
            final UnsafeBuffer frame = sbe();
            frame.putShort(offset, (short)(frame.getShort(offset, ByteOrder.LITTLE_ENDIAN) + 1), ByteOrder.LITTLE_ENDIAN);
            assertThat(decoder.decode(frame, 0, frame.capacity(), 2, 2, 3)).as("offset %s", offset).isFalse();
        }
        for (final int offset : new int[] {8, 16, 24, 38, 46}) {
            final UnsafeBuffer frame = sbe();
            frame.putLong(offset, Long.MIN_VALUE, ByteOrder.LITTLE_ENDIAN);
            assertThat(decoder.decode(frame, 0, frame.capacity(), 2, 2, 3)).isFalse();
        }
        for (final int offset : new int[] {32, 33}) {
            final UnsafeBuffer frame = sbe();
            frame.putByte(offset, (byte)-128);
            assertThat(decoder.decode(frame, 0, frame.capacity(), 2, 2, 3)).isFalse();
        }
        final UnsafeBuffer frame = sbe();
        frame.putByte(frame.capacity() - 1, (byte)'X');
        assertThat(decoder.decode(frame, 0, frame.capacity(), 2, 2, 3)).isFalse();
        assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.SYMBOL);
        assertThat(decoder.decode(sbe(), 0, sbe().capacity(), 2, 0, 3)).isFalse();
        assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.OVERFLOW);
        assertThat(decoder.decode(sbe(), 0, sbe().capacity(), 2, 18, 18)).isFalse();
        final UnsafeBuffer oversized = sbe();
        oversized.putShort(36, (short)(BinanceDepthDecoder.MAX_LEVELS + 1), ByteOrder.LITTLE_ENDIAN);
        assertThat(decoder.decode(oversized, 0, oversized.capacity(), 2, 2, 3)).isFalse();
        assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.CAPACITY);
        final UnsafeBuffer trailing = new UnsafeBuffer(Arrays.copyOf(sbe().byteArray(), sbe().capacity() + 1));
        assertThat(decoder.decode(trailing, 0, trailing.capacity(), 2, 2, 3)).isFalse();
    }

    @Test
    void fiveThousandLevelRestSnapshotCanUseTheWholeDeclaredHalfMegabyteBuffer() {
        final StringBuilder text = new StringBuilder("{\"lastUpdateId\":100,\"bids\":[");
        levels(text, 5000, 100_000);
        text.append("],\"asks\":[");
        levels(text, 5000, 200_000);
        text.append("]}");
        while (text.length() < 512 * 1024) { text.append(' '); }
        assertThat(decode(text.toString(), 3, 0, 0)).isTrue();
        assertThat(decoder.bidCount).isEqualTo(5000);
        assertThat(decoder.askCount).isEqualTo(5000);
        assertThat(decoder.bidPrices[4999]).isEqualTo(104_999);
        assertThat(decoder.askPrices[4999]).isEqualTo(204_999);
        final StringBuilder oversized = new StringBuilder("{\"lastUpdateId\":100,\"bids\":[");
        levels(oversized, BinanceDepthDecoder.MAX_LEVELS + 1, 100_000);
        oversized.append("],\"asks\":[]}");
        assertThat(decode(oversized.toString(), 3, 0, 0)).isFalse();
        assertThat(decoder.error).isEqualTo(BinanceDepthDecoder.CAPACITY);
        assertThat(decode(JSON, 1, 2, 3)).isTrue();
    }

    @Test
    void actualSuccessfulAndRejectedDecodesAllocateNothingAfterWarmup() {
        final java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assertThat(management).isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean)management;
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        final UnsafeBuffer json = utf8(JSON), binary = sbe(), snapshot = utf8(snapshot("1.2300", "0"));
        final UnsafeBuffer malformed = utf8(JSON.replace("\"U\":500", "\"U\":9223372036854775808"));
        final UnsafeBuffer duplicate = utf8(JSON.replace("[\"1000.00\",\"0\"]", "[\"1001.23\",\"0\"]"));
        final Runnable action = () -> {
            require(decoder.decode(json, 0, json.capacity(), 1, 2, 3));
            consumed ^= decoder.bidPrices[0];
            require(decoder.decode(binary, 0, binary.capacity(), 2, 2, 3));
            consumed ^= decoder.askSizes[0];
            require(decoder.decode(snapshot, 0, snapshot.capacity(), 3, 2, 3));
            require(!decoder.decode(malformed, 0, malformed.capacity(), 1, 2, 3));
            require(!decoder.decode(duplicate, 0, duplicate.capacity(), 1, 2, 3));
        };
        for (int i = 0; i < 100_000; i++) { action.run(); }
        allocated(bean, thread, action, 32);
        assertThat(allocated(bean, thread, action, 10_000)).isZero();
        assertThat(allocated(bean, thread, action, 20_000)).isZero();
        assertThat(allocated(bean, thread, () -> escaping = new byte[64], 1024)).isGreaterThanOrEqualTo(65_536);
        escaping = null;
    }

    private static long allocated(final ThreadMXBean bean, final long thread, final Runnable action, final int count) {
        final long before = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < count; i++) { action.run(); }
        final long after = bean.getThreadAllocatedBytes(thread);
        require(before >= 0 && after >= before);
        return after - before;
    }

    private void assertFixture() {
        assertThat(decoder.error).isZero();
        assertThat(decoder.firstId).isEqualTo(500);
        assertThat(decoder.lastId).isEqualTo(505);
        assertThat(decoder.bidCount).isEqualTo(2);
        assertThat(decoder.askCount).isEqualTo(1);
        assertThat(Arrays.copyOf(decoder.bidPrices, 2)).containsExactly(100123, 100000);
        assertThat(Arrays.copyOf(decoder.bidSizes, 2)).containsExactly(4567, 0);
        assertThat(decoder.askPrices[0]).isEqualTo(100456);
        assertThat(decoder.askSizes[0]).isEqualTo(25);
    }

    private void assertRejectedScratch() {
        assertThat(decoder.error).isNotZero();
        assertThat(decoder.bidCount).isZero();
        assertThat(decoder.askCount).isZero();
        assertThat(decoder.firstId).isZero();
        assertThat(decoder.lastId).isZero();
    }

    private boolean decode(final String text, final int format, final int priceScale, final int quantityScale) {
        final UnsafeBuffer input = utf8(text);
        return decoder.decode(input, 0, input.capacity(), format, priceScale, quantityScale);
    }

    private static String snapshot(final String price, final String size) {
        return "{\"lastUpdateId\":1,\"bids\":[[\"" + price + "\",\"" + size + "\"]],\"asks\":[]}";
    }

    private static void levels(final StringBuilder text, final int count, final int firstPrice) {
        for (int i = 0; i < count; i++) {
            if (i != 0) { text.append(','); }
            text.append("[\"").append(firstPrice + i).append("\",\"1\"]");
        }
    }

    private static UnsafeBuffer utf8(final String text) { return new UnsafeBuffer(text.getBytes(StandardCharsets.UTF_8)); }
    private static UnsafeBuffer sbe() { return new UnsafeBuffer(HexFormat.of().parseHex(SBE_HEX)); }
    private static void require(final boolean condition) { if (!condition) { throw new AssertionError("decoder contract failed"); } }
}
