package io.github.jstadeusiak.spine.examples.binance;

import io.github.jstadeusiak.spine.sequencer.SequencerCore;
import io.github.jstadeusiak.spine.sequencer.SnapshotChecksum;

import io.github.jstadeusiak.spine.protocol.CaptureCodec;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.protocol.market.sbe.BookResultDecoder;
import io.github.jstadeusiak.spine.protocol.market.sbe.BookSourceEncoder;
import io.github.jstadeusiak.spine.protocol.market.sbe.BootstrapChunkEncoder;
import io.github.jstadeusiak.spine.protocol.market.sbe.MarketSnapshotDecoder;
import io.github.jstadeusiak.spine.protocol.market.sbe.MessageHeaderEncoder;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MarketCoreTest {
    static final long LOG = 11, UNIT = 100_000_000L;
    static final int JSON_SOURCE = 1, SBE_SOURCE = 2, BOOTSTRAP_SOURCE = 3;
    static final String SNAPSHOT = "{\"lastUpdateId\":100,\"bids\":[[\"100\",\"1\"],[\"99\",\"2\"]]," +
        "\"asks\":[[\"101\",\"3\"],[\"102\",\"4\"]]}";

    @Test
    void oneCommittedCapturedUpdateProducesExactBboAndPreservesItsEntireOriginalCapture() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final UnsafeBuffer proposal = inputs.observation(JSON_SOURCE, 1, 1,
            utf8(diff(101, 102, "[[\"100\",\"1.25\"],[\"100.50\",\"2\"],[\"99\",\"0\"]]",
                "[[\"101\",\"0\"],[\"101.50\",\"4\"]]")));
        send(core, proposal, 12345);
        final BinanceBook book = ((BinanceApplication)core.application()).book;
        assertThat(book.nativeFrontier).isEqualTo(102);
        assertThat(book.status).isEqualTo(BinanceBook.VALID);
        assertThat(book.bids.size()).isEqualTo(2);
        assertThat(book.bids.get(100 * UNIT)).isEqualTo(125_000_000);
        assertThat(book.bids.get(99 * UNIT)).isEqualTo(-1);
        assertThat(book.asks.get(101 * UNIT)).isEqualTo(-1);
        assertThat(book.bidPrice).isEqualTo(10_050_000_000L);
        assertThat(book.bidSize).isEqualTo(2 * UNIT);
        assertThat(book.askPrice).isEqualTo(10_150_000_000L);
        assertThat(book.askSize).isEqualTo(4 * UNIT);
        assertThat(book.eligible()).isTrue();
        final BookResultDecoder result = result(core);
        assertThat(result.venueId()).isEqualTo(1);
        assertThat(result.instrumentId()).isEqualTo(1);
        assertThat(result.epoch()).isEqualTo(1);
        assertThat(result.frontier()).isEqualTo(102);
        assertThat(result.lastAppliedTimeNanos()).isEqualTo(12345);
        assertThat(result.bidPrice()).isEqualTo(book.bidPrice);
        assertThat(result.askPrice()).isEqualTo(book.askPrice);
        assertThat(result.bidFloor()).isEqualTo(99 * UNIT);
        assertThat(result.askCeiling()).isEqualTo(102 * UNIT);
        assertThat((int)result.priceScale()).isEqualTo(8);
        assertThat((int)result.quantityScale()).isEqualTo(8);
        assertOriginal(core, proposal);
        final Wire wire = new Wire();
        assertThat(wire.validEvent(core.resultBuffer(), core.eventOffset(), core.eventLength())).isTrue();
        assertThat(wire.event.sequencedTimeNanos()).isEqualTo(12345);
        assertThat(wire.event.sourceId()).isEqualTo(JSON_SOURCE);
        assertThat(wire.event.sourceConfigVersion()).isEqualTo(1);
        assertThat(wire.event.clockId()).isEqualTo(1);
        assertThat(wire.event.receiveTimeNanos()).isEqualTo(10);
        assertThat(wire.event.completeTimeNanos()).isEqualTo(11);
        assertThat(wire.event.readyTimeNanos()).isEqualTo(12);
    }

    @Test
    void redundantNativeUpdatesRemainRecordedAndNeverRollBackTheBook() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        send(core, inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(101, 101, oneBid("1"), "[]"))), 2000);
        final UnsafeBuffer redundant = inputs.observation(SBE_SOURCE, 1, 1, sbe(101, 101, 1));
        send(core, redundant, 3000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(101);
        assertThat(((BinanceApplication)core.application()).book.lastTimeNanos).isEqualTo(2000);
        assertThat(((BinanceApplication)core.application()).book.status).isEqualTo(BinanceBook.VALID);
        assertThat((int)result(core).outcome()).isEqualTo(BinanceBook.STALE);
        assertOriginal(core, redundant);
        send(core, inputs.observation(SBE_SOURCE, 1, 1, sbe(102, 102, 2)), 4000);
        send(core, inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(99, 100, oneBid("99"), "[]"))), 5000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(102);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(2 * UNIT);
        assertThat(((BinanceApplication)core.application()).book.lastTimeNanos).isEqualTo(4000);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
    }

    @Test
    void nativeConflictDisablesPricingAndOnlyAQualifiedNewBootstrapRepairsIt() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        send(core, inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(101, 101, oneBid("1"), "[]"))), 2000);
        send(core, inputs.observation(SBE_SOURCE, 1, 1, sbe(101, 101, 2)), 3000);
        assertThat(((BinanceApplication)core.application()).book.status).isEqualTo(BinanceBook.CONFLICT);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(101);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(UNIT);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
        send(core, inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(102, 102, oneBid("3"), "[]"))), 4000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(101);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
        bootstrap(core, inputs, 2, SNAPSHOT.replace(":100,", ":101,"), 5000);
        assertThat(((BinanceApplication)core.application()).book.status).isEqualTo(BinanceBook.VALID);
        send(core, inputs.observation(SBE_SOURCE, 1, 1, sbe(102, 102, 3)), 6000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(102);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(3 * UNIT);
    }

    @Test
    void alternateSourceCanRepairAGapAndConsumeTheBoundedPendingNativeRange() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final UnsafeBuffer pending = inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(103, 103, oneBid("3"), "[]")));
        send(core, pending, 2000);
        assertThat(((BinanceApplication)core.application()).book.status).isEqualTo(BinanceBook.GAP);
        assertThat(((BinanceApplication)core.application()).book.pendingCount).isEqualTo(1);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
        final UnsafeBuffer state = snapshot(core);
        final SequencerCore restored = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        restored.restore(state, 0, state.capacity());
        final UnsafeBuffer repair = inputs.observation(SBE_SOURCE, 1, 1, sbe(101, 102, 2));
        send(core, repair, 3000);
        send(restored, repair, 3000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(103);
        assertThat(((BinanceApplication)core.application()).book.pendingCount).isZero();
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(3 * UNIT);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        assertThat(canonical(restored)).isEqualTo(canonical(core));
        assertThat(snapshot(restored).byteArray()).isEqualTo(snapshot(core).byteArray());
    }

    @Test
    void producerRetryRetainsItsOriginalMarketResultAfterAnotherProducerChangedTheBookAndAfterRestore() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final UnsafeBuffer json = inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(101, 101, oneBid("1"), "[]")));
        send(core, json, 2000);
        final byte[] original = canonical(core);
        send(core, inputs.observation(SBE_SOURCE, 1, 1, sbe(102, 102, 2)), 3000);
        final long head = core.sequence();
        final UnsafeBuffer state = snapshot(core);
        assertThat(core.onCommand(json, 0, json.capacity(), 9000, 1)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(core.eventLength()).isZero();
        assertThat(canonical(core)).isEqualTo(original);
        assertThat(core.sequence()).isEqualTo(head);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(102);
        assertThat(result(core).frontier()).isEqualTo(101);
        assertThat(result(core).lastAppliedTimeNanos()).isEqualTo(2000);
        final SequencerCore restored = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        restored.restore(state, 0, state.capacity());
        assertThat(restored.onCommand(json, 0, json.capacity(), 9999, 1)).isEqualTo(AdmissionStatus.DUPLICATE);
        assertThat(canonical(restored)).isEqualTo(original);
        assertThat(((BinanceApplication)restored.application()).book.nativeFrontier).isEqualTo(102);
    }

    @Test
    void sourceReconnectWithinEpochPreservesStateAndNewEpochRequiresACompleteBootstrapAndRebindings() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        send(core, inputs.bind(JSON_SOURCE, 1, 1, 2, 2), 2000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        send(core, inputs.observation(JSON_SOURCE, 2, 2, utf8(diff(101, 101, oneBid("2"), "[]"))), 3000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(101);
        send(core, inputs.bind(JSON_SOURCE, 1, 2, 3, 3), 4000);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
        assertThat(((BinanceApplication)core.application()).book.bids.size()).isZero();
        assertThat(((BinanceApplication)core.application()).book.asks.size()).isZero();
        send(core, inputs.bind(SBE_SOURCE, 2, 2, 2, 2), 5000);
        send(core, inputs.bind(BOOTSTRAP_SOURCE, 3, 2, 2, 2), 6000);
        final byte[] raw = utf8(SNAPSHOT.replace(":100,", ":200,"));
        send(core, inputs.chunk(BOOTSTRAP_SOURCE, 2, 2, 2, raw.length, 0, raw), 7000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(200);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        send(core, inputs.observation(SBE_SOURCE, 2, 2, sbe(201, 201, 4)), 8000);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(4 * UNIT);
        send(core, inputs.bind(JSON_SOURCE, 1, 1, 4, 4), 9000);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(201);
    }

    @Test
    void eachInputCutRestoresCompleteMarketStatePendingWorkAndProducerReceipts() {
        final List<UnsafeBuffer> commands = replayInputs();
        final SequencerCore baseline = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        final List<byte[]> expected = new ArrayList<>();
        for (int i = 0; i < commands.size(); i++) {
            apply(baseline, commands.get(i), i);
            expected.add(bytes(baseline.resultBuffer(), 0, baseline.resultLength()));
        }
        final byte[] expectedState = snapshot(baseline).byteArray();
        for (int cut = 0; cut <= commands.size(); cut++) {
            final SequencerCore prefix = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
            for (int i = 0; i < cut; i++) { apply(prefix, commands.get(i), i); }
            final UnsafeBuffer state = snapshot(prefix);
            final SequencerCore restored = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
            restored.restore(state, 0, state.capacity());
            for (int i = cut; i < commands.size(); i++) {
                apply(restored, commands.get(i), i);
                assertThat(bytes(restored.resultBuffer(), 0, restored.resultLength())).as("cut %s input %s", cut, i)
                    .isEqualTo(expected.get(i));
            }
            assertThat(snapshot(restored).byteArray()).as("cut %s", cut).isEqualTo(expectedState);
        }
    }

    @Test
    void bootstrapInstallationIsAtomicAndAnInProgressFragmentSequenceSurvivesCoreRestore() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final byte[] raw = utf8(SNAPSHOT.replace(":100,", ":200,").replace("\"100\",\"1\"", "\"100\",\"9\""));
        final int split = raw.length / 2;
        send(core, inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, 2, raw.length, 0, Arrays.copyOf(raw, split)), 2000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(UNIT);
        final UnsafeBuffer state = snapshot(core);
        final SequencerCore restored = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        restored.restore(state, 0, state.capacity());
        final UnsafeBuffer end = inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, 2, raw.length, split,
            Arrays.copyOfRange(raw, split, raw.length));
        send(core, end, 3000);
        send(restored, end, 3000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(200);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(9 * UNIT);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        assertThat(canonical(restored)).isEqualTo(canonical(core));
        assertThat(snapshot(restored).byteArray()).isEqualTo(snapshot(core).byteArray());
    }

    @Test
    void malformedCompletedBootstrapAndMalformedBoundDiffLeaveTheExistingLevelsUnchanged() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final byte[] malformed = utf8(SNAPSHOT.replace("\"100\",\"1\"", "\"100\",\"9223372036854775808\""));
        send(core, inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, 2, malformed.length, 0, malformed), 2000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.bids.get(100 * UNIT)).isEqualTo(UNIT);
        assertThat(((BinanceApplication)core.application()).book.asks.get(101 * UNIT)).isEqualTo(3 * UNIT);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
        bootstrap(core, inputs, 3, SNAPSHOT, 3000);
        final UnsafeBuffer badDiff = inputs.observation(JSON_SOURCE, 1, 1,
            utf8(diff(101, 101, "[[\"100\",\"1\"],[\"100.0\",\"2\"]]", "[]")));
        send(core, badDiff, 4000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(UNIT);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
        assertOriginal(core, badDiff);
    }

    @Test
    void knownSourceScopeMismatchInvalidatesEligibilityWhileUnboundSourcesKeepTheirRawOutput() {
        for (final int[] metadata : new int[][] {{2, 1}, {1, 2}}) {
            final Inputs inputs = new Inputs();
            final SequencerCore core = ready(inputs);
            final UnsafeBuffer mismatch = inputs.observation(JSON_SOURCE, metadata[0], metadata[1],
                utf8(diff(101, 101, oneBid("9"), "[]")));
            send(core, mismatch, 2000);
            assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
            assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(UNIT);
            assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
            assertOriginal(core, mismatch);
        }
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final UnsafeBuffer unbound = inputs.observation(99, 1, 1, utf8(diff(101, 101, oneBid("9"), "[]")));
        send(core, unbound, 2000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        assertThat(bytes(core.resultBuffer(), core.eventOffset() + Wire.EVENT_OVERHEAD,
            core.eventLength() - Wire.EVENT_OVERHEAD))
            .isEqualTo(bytes(unbound, Wire.PROPOSAL_OVERHEAD, unbound.capacity() - Wire.PROPOSAL_OVERHEAD));
        final UnsafeBuffer changedScale = inputs.bind(JSON_SOURCE, 1, 1, 2, 2);
        changedScale.putByte(Wire.PROPOSAL_OVERHEAD + 8 + BookSourceEncoder.priceScaleEncodingOffset(), (byte)9);
        send(core, changedScale, 3000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.bidSize).isEqualTo(UNIT);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
    }

    @Test
    void pricingEligibilityUsesLoggedTimeAndDoesNotRefreshTheBookOnControlOrRedundantMessages() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final long applied = ((BinanceApplication)core.application()).book.lastTimeNanos;
        final byte[] ack = utf8("{\"result\":null,\"id\":1}");
        send(core, inputs.observation(JSON_SOURCE, 1, 1, ack), applied + 1_000_000_000);
        assertThat((int)result(core).status()).isEqualTo(BinanceBook.VALID);
        assertThat(result(core).bidPrice()).isEqualTo(100 * UNIT);
        send(core, inputs.observation(JSON_SOURCE, 1, 1, ack), applied + 1_000_000_001);
        assertThat(result(core).bidPrice()).isZero();
        assertThat(result(core).bidSize()).isZero();
        assertThat(result(core).askPrice()).isZero();
        assertThat(result(core).askSize()).isZero();
        assertThat(((BinanceApplication)core.application()).book.lastTimeNanos).isEqualTo(applied);
        assertThat(((BinanceApplication)core.application()).book.bidPrice).isEqualTo(100 * UNIT);
        send(core, inputs.observation(SBE_SOURCE, 1, 1, sbe(99, 100, 1)), applied + 1_000_000_002);
        assertThat(result(core).bidPrice()).isZero();
        assertThat(((BinanceApplication)core.application()).book.lastTimeNanos).isEqualTo(applied);
        send(core, inputs.observation(SBE_SOURCE, 1, 1, sbe(101, 101, 2)), applied + 1_000_000_003);
        assertThat((int)result(core).status()).isEqualTo(BinanceBook.VALID);
        assertThat(result(core).bidPrice()).isEqualTo(100 * UNIT);
        assertThat(result(core).bidSize()).isEqualTo(2 * UNIT);
        assertThat(((BinanceApplication)core.application()).book.lastTimeNanos).isEqualTo(applied + 1_000_000_003);
    }

    @Test
    void sbeInterpretationRequiresTheCollectorsExactTagAndRecognizesOnlyTaggedTextAcknowledgements() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        final byte[] text = utf8("{\"result\":null,\"id\":1}");
        final byte[] acknowledgement = new byte[text.length + 1];
        System.arraycopy(text, 0, acknowledgement, 1, text.length);
        send(core, inputs.observation(SBE_SOURCE, 1, 1, acknowledgement), 2000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        assertThat(((BinanceApplication)core.application()).book.lastTimeNanos).isEqualTo(1003);
        final byte[] binary = sbe(101, 101, 2);
        for (final byte[] invalid : new byte[][] {Arrays.copyOfRange(binary, 1, binary.length), text,
            new byte[] {1}, new byte[] {2, 1}}) {
            final Inputs malformedInputs = new Inputs();
            final SequencerCore malformed = ready(malformedInputs);
            send(malformed, malformedInputs.observation(SBE_SOURCE, 1, 1, invalid), 3000);
            assertThat(((BinanceApplication)malformed.application()).book.nativeFrontier).isEqualTo(100);
            assertThat(((BinanceApplication)malformed.application()).book.eligible()).isFalse();
        }
        final Inputs mistaggedInputs = new Inputs();
        final SequencerCore mistagged = ready(mistaggedInputs);
        binary[0] = 0;
        send(mistagged, mistaggedInputs.observation(SBE_SOURCE, 1, 1, binary), 3000);
        assertThat(((BinanceApplication)mistagged.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)mistagged.application()).book.eligible()).isFalse();
    }

    @Test
    void maximumBootstrapFragmentsPreserveTheirBytesAndInstallOnlyAtTheFinalCompleteFragment() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        initialize(core);
        send(core, inputs.bind(BOOTSTRAP_SOURCE, 3, 1, 1, 1), 1000);
        final byte[] raw = Arrays.copyOf(utf8(SNAPSHOT), 512 * 1024);
        Arrays.fill(raw, utf8(SNAPSHOT).length, raw.length, (byte)' ');
        final int maximum = Wire.MAX_PAYLOAD_BYTES - 28;
        for (int offset = 0; offset < raw.length;) {
            final int size = Math.min(maximum, raw.length - offset);
            final UnsafeBuffer chunk = inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, 1, raw.length, offset,
                Arrays.copyOfRange(raw, offset, offset + size));
            send(core, chunk, 2000 + offset);
            assertThat(chunk.capacity()).isLessThanOrEqualTo(Wire.MAX_PROPOSAL_BYTES);
            assertThat(core.eventLength()).isLessThanOrEqualTo(Wire.MAX_EVENT_BYTES);
            assertThat(new Wire().validEvent(core.resultBuffer(), core.eventOffset(), core.eventLength())).isTrue();
            assertOriginal(core, chunk);
            offset += size;
            assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(offset == raw.length ? 100 : 0);
        }
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        final UnsafeBuffer invalid = inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, 2, raw.length + 1, 0, new byte[] {'{'});
        send(core, invalid, 900000);
        assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(100);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isFalse();
    }

    @Test
    void corruptOuterReceiptsOrMarketSnapshotDoNotReplaceAnExistingLiveCore() {
        final Inputs inputs = new Inputs();
        final SequencerCore core = ready(inputs);
        send(core, inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(101, 101, oneBid("2"), "[]"))), 2000);
        final UnsafeBuffer baseline = snapshot(core);
        final Wire snapshotWire = new Wire();
        assertThat(snapshotWire.validSnapshot(baseline, 0, baseline.capacity())).isTrue();
        final int market = snapshotWire.applicationStateOffset + 4;
        final int[] offsets = {6, 70, Wire.SNAPSHOT_OVERHEAD + 64, market + 4, market + 6};
        for (final int changed : offsets) {
            final UnsafeBuffer corrupt = new UnsafeBuffer(baseline.byteArray().clone());
            corrupt.putByte(changed, (byte)(corrupt.getByte(changed) + 1));
            SnapshotChecksum.seal(corrupt);
            assertThatThrownBy(() -> core.restore(corrupt, 0, corrupt.capacity())).isInstanceOf(IllegalArgumentException.class);
            assertThat(snapshot(core).byteArray()).as("corruption offset %s", changed).isEqualTo(baseline.byteArray());
            assertThat(((BinanceApplication)core.application()).book.nativeFrontier).isEqualTo(101);
            assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        }
        final UnsafeBuffer truncated = new UnsafeBuffer(Arrays.copyOf(baseline.byteArray(), baseline.capacity() - 1));
        assertThatThrownBy(() -> core.restore(truncated, 0, truncated.capacity())).isInstanceOf(IllegalArgumentException.class);
        assertThat(snapshot(core).byteArray()).isEqualTo(baseline.byteArray());

        // Each latest producer receipt carries a cached fixed BookResult header and its scalar fields.
        final int receipt = Wire.SNAPSHOT_OVERHEAD;
        final int[] cachedBytes = {receipt + 157, receipt + 158, receipt + 159};
        for (final int changed : cachedBytes) {
            final UnsafeBuffer corrupt = new UnsafeBuffer(baseline.byteArray().clone());
            corrupt.putByte(changed, (byte)(changed == receipt + 157 ? 7 : 19));
            SnapshotChecksum.seal(corrupt);
            assertThatThrownBy(() -> core.restore(corrupt, 0, corrupt.capacity())).isInstanceOf(IllegalArgumentException.class);
            assertThat(snapshot(core).byteArray()).isEqualTo(baseline.byteArray());
        }
        final UnsafeBuffer negativeFrontier = new UnsafeBuffer(baseline.byteArray().clone());
        negativeFrontier.putLong(receipt + 92, -1, Wire.ORDER);
        SnapshotChecksum.seal(negativeFrontier);
        assertThatThrownBy(() -> core.restore(negativeFrontier, 0, negativeFrontier.capacity()))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(snapshot(core).byteArray()).isEqualTo(baseline.byteArray());

        // Covered rows used for duplicate comparison must still agree with the live book.
        final int sources = market + 8 + MarketSnapshotDecoder.BLOCK_LENGTH;
        final int levels = sources + 4 + 13 * (baseline.getShort(sources + 2, Wire.ORDER) & 65535);
        final int last = levels + 4 + 17 * (baseline.getShort(levels + 2, Wire.ORDER) & 65535) + 4;
        final UnsafeBuffer mismatchedLastSize = new UnsafeBuffer(baseline.byteArray().clone());
        mismatchedLastSize.putLong(last + 9, 99 * UNIT, Wire.ORDER);
        SnapshotChecksum.seal(mismatchedLastSize);
        assertThatThrownBy(() -> core.restore(mismatchedLastSize, 0, mismatchedLastSize.capacity()))
            .isInstanceOf(IllegalArgumentException.class);
        assertThat(snapshot(core).byteArray()).isEqualTo(baseline.byteArray());

        // A valid bound-but-empty market is impossible at canonical sequence zero.
        final SequencerCore pristine = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        initialize(pristine);
        final UnsafeBuffer empty = snapshot(pristine);
        send(pristine, new Inputs().bind(JSON_SOURCE, 1, 1, 1, 1), 1000);
        final UnsafeBuffer bound = snapshot(pristine);
        assertThat(snapshotWire.validSnapshot(bound, 0, bound.capacity())).isTrue();
        final int boundMarket = snapshotWire.applicationStateOffset;
        final int boundLength = bound.getInt(boundMarket, Wire.ORDER);
        final UnsafeBuffer fabricated = new UnsafeBuffer(new byte[Wire.SNAPSHOT_OVERHEAD + 4 + boundLength]);
        fabricated.putBytes(0, empty, 0, Wire.SNAPSHOT_OVERHEAD);
        fabricated.putInt(Wire.SNAPSHOT_OVERHEAD, boundLength, Wire.ORDER);
        fabricated.putBytes(Wire.SNAPSHOT_OVERHEAD + 4, bound, boundMarket + 4, boundLength);
        SnapshotChecksum.seal(fabricated);
        assertThatThrownBy(() -> core.restore(fabricated, 0, fabricated.capacity())).isInstanceOf(IllegalArgumentException.class);
        assertThat(snapshot(core).byteArray()).isEqualTo(baseline.byteArray());
    }

    static SequencerCore ready(final Inputs inputs) {
        final SequencerCore core = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        initialize(core);
        send(core, inputs.bind(JSON_SOURCE, 1, 1, 1, 1), 1000);
        send(core, inputs.bind(SBE_SOURCE, 2, 1, 1, 1), 1001);
        send(core, inputs.bind(BOOTSTRAP_SOURCE, 3, 1, 1, 1), 1002);
        bootstrap(core, inputs, 1, SNAPSHOT, 1003);
        assertThat(((BinanceApplication)core.application()).book.eligible()).isTrue();
        return core;
    }

    static void bootstrap(final SequencerCore core, final Inputs inputs, final long id, final String snapshot, final long time) {
        final byte[] raw = utf8(snapshot);
        send(core, inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, id, raw.length, 0, raw), time);
    }

    static void initialize(final SequencerCore core) {
        final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        new Wire().encodeInitialize(input, 0, LOG, BinanceApplication.IDENTITY);
        assertThat(core.onCommand(input, 0, input.capacity(), 0, 1)).isEqualTo(AdmissionStatus.INITIALIZED);
    }

    static void send(final SequencerCore core, final UnsafeBuffer input, final long time) {
        assertThat(core.onCommand(input, 0, input.capacity(), time, 1)).isEqualTo(AdmissionStatus.ACCEPTED);
        assertThat(core.eventLength()).isPositive();
    }

    static UnsafeBuffer snapshot(final SequencerCore core) {
        final int length = core.snapshot();
        return new UnsafeBuffer(bytes(core.resultBuffer(), 0, length));
    }

    static byte[] canonical(final SequencerCore core) {
        return bytes(core.resultBuffer(), Wire.RESULT_OVERHEAD, core.resultLength() - Wire.RESULT_OVERHEAD);
    }

    static byte[] bytes(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] result = new byte[length];
        buffer.getBytes(offset, result);
        return result;
    }

    private static BookResultDecoder result(final SequencerCore core) {
        final int offset = core.eventOffset() + Wire.EVENT_OVERHEAD;
        assertThat(core.resultBuffer().getShort(offset, Wire.ORDER) & 65535).isEqualTo(84);
        assertThat(core.resultBuffer().getShort(offset + 2, Wire.ORDER) & 65535).isEqualTo(3);
        assertThat(core.resultBuffer().getShort(offset + 4, Wire.ORDER) & 65535).isEqualTo(401);
        assertThat(core.resultBuffer().getShort(offset + 6, Wire.ORDER)).isEqualTo((short)1);
        return new BookResultDecoder().wrap(core.resultBuffer(), offset + 8, BookResultDecoder.BLOCK_LENGTH, BookResultDecoder.SCHEMA_VERSION);
    }

    private static void assertOriginal(final SequencerCore core, final UnsafeBuffer proposal) {
        final BookResultDecoder result = result(core);
        final byte[] original = new byte[result.observationLength()];
        result.getObservation(original, 0, original.length);
        assertThat(original).isEqualTo(bytes(proposal, Wire.PROPOSAL_OVERHEAD, proposal.capacity() - Wire.PROPOSAL_OVERHEAD));
    }

    static String diff(final long first, final long last, final String bids, final String asks) {
        return "{\"e\":\"depthUpdate\",\"E\":1700000000000000,\"s\":\"BTCUSDT\",\"U\":" + first +
            ",\"u\":" + last + ",\"b\":" + bids + ",\"a\":" + asks + "}";
    }

    static String oneBid(final String size) { return "[[\"100\",\"" + size + "\"]]"; }
    static byte[] utf8(final String text) { return text.getBytes(StandardCharsets.UTF_8); }

    static byte[] sbe(final long first, final long last, final long size) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[66]);
        buffer.putShort(0, (short)26, Wire.ORDER);
        buffer.putShort(2, (short)10003, Wire.ORDER);
        buffer.putShort(4, (short)1, Wire.ORDER);
        buffer.putLong(8, 1_700_000_000_000_000L, Wire.ORDER);
        buffer.putLong(16, first, Wire.ORDER);
        buffer.putLong(24, last, Wire.ORDER);
        buffer.putShort(34, (short)16, Wire.ORDER);
        buffer.putShort(36, (short)1, Wire.ORDER);
        buffer.putLong(38, 100, Wire.ORDER);
        buffer.putLong(46, size, Wire.ORDER);
        buffer.putShort(54, (short)16, Wire.ORDER);
        buffer.putByte(58, (byte)7);
        buffer.putBytes(59, utf8("BTCUSDT"));
        final byte[] tagged = new byte[buffer.capacity() + 1];
        tagged[0] = 1;
        buffer.getBytes(0, tagged, 1, buffer.capacity());
        return tagged;
    }

    private static List<UnsafeBuffer> replayInputs() {
        final Inputs inputs = new Inputs();
        final List<UnsafeBuffer> commands = new ArrayList<>();
        final UnsafeBuffer init = new UnsafeBuffer(new byte[Wire.INITIALIZE_LENGTH]);
        inputs.wire.encodeInitialize(init, 0, LOG, BinanceApplication.IDENTITY);
        commands.add(init);
        commands.add(inputs.bind(JSON_SOURCE, 1, 1, 1, 1));
        commands.add(inputs.bind(SBE_SOURCE, 2, 1, 1, 1));
        commands.add(inputs.bind(BOOTSTRAP_SOURCE, 3, 1, 1, 1));
        final byte[] raw = utf8(SNAPSHOT);
        final int split = raw.length / 2;
        commands.add(inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, 1, raw.length, 0, Arrays.copyOf(raw, split)));
        commands.add(inputs.chunk(BOOTSTRAP_SOURCE, 1, 1, 1, raw.length, split, Arrays.copyOfRange(raw, split, raw.length)));
        commands.add(inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(101, 101, oneBid("1"), "[]"))));
        commands.add(inputs.observation(SBE_SOURCE, 1, 1, sbe(101, 101, 1)));
        commands.add(inputs.observation(JSON_SOURCE, 1, 1, utf8(diff(103, 103, oneBid("3"), "[]"))));
        commands.add(inputs.observation(SBE_SOURCE, 1, 1, sbe(102, 102, 2)));
        commands.add(inputs.bind(JSON_SOURCE, 1, 1, 2, 2));
        commands.add(inputs.observation(JSON_SOURCE, 2, 2, utf8(diff(104, 104, oneBid("4"), "[]"))));
        commands.add(inputs.bind(JSON_SOURCE, 1, 2, 3, 3));
        commands.add(inputs.bind(SBE_SOURCE, 2, 2, 2, 2));
        commands.add(inputs.bind(BOOTSTRAP_SOURCE, 3, 2, 2, 2));
        final byte[] next = utf8(SNAPSHOT.replace(":100,", ":200,"));
        commands.add(inputs.chunk(BOOTSTRAP_SOURCE, 2, 2, 2, next.length, 0, Arrays.copyOf(next, split)));
        commands.add(inputs.chunk(BOOTSTRAP_SOURCE, 2, 2, 2, next.length, split, Arrays.copyOfRange(next, split, next.length)));
        commands.add(inputs.observation(SBE_SOURCE, 2, 2, sbe(201, 201, 5)));
        return commands;
    }

    private static void apply(final SequencerCore core, final UnsafeBuffer input, final int index) {
        assertThat(core.onCommand(input, 0, input.capacity(), 1000 + index, 1))
            .isEqualTo(index == 0 ? AdmissionStatus.INITIALIZED : AdmissionStatus.ACCEPTED);
    }

    static final class Inputs {
        final Wire wire = new Wire();
        final CaptureCodec capture = new CaptureCodec();
        final MessageHeaderEncoder header = new MessageHeaderEncoder();
        final BookSourceEncoder source = new BookSourceEncoder();
        final BootstrapChunkEncoder chunk = new BootstrapChunkEncoder();
        final long[] submitted = new long[256], frames = new long[256];

        UnsafeBuffer bind(final int sourceId, final int format, final long epoch, final int configuration, final int clock) {
            final UnsafeBuffer payload = new UnsafeBuffer(new byte[8 + BookSourceEncoder.BLOCK_LENGTH]);
            source.wrapAndApplyHeader(payload, 0, header).venueId(1).instrumentId(1).epoch(epoch).format((short)format)
                .priceScale((short)8).quantityScale((short)8).maximumAgeNanos(1_000_000_000);
            return proposal(sourceId, configuration, clock, payload);
        }

        UnsafeBuffer observation(final int sourceId, final int configuration, final int clock, final byte[] raw) {
            final UnsafeBuffer payload = new UnsafeBuffer(new byte[CaptureCodec.OBSERVATION_OVERHEAD + raw.length]);
            capture.encodeObservation(payload, 0, ++frames[sourceId], 0, CaptureCodec.UNKNOWN_NATIVE_ID,
                payload, 0, 0, new UnsafeBuffer(raw), 0, raw.length);
            return proposal(sourceId, configuration, clock, payload);
        }

        UnsafeBuffer chunk(final int sourceId, final int configuration, final int clock, final long bootstrapId,
            final int totalLength, final int chunkOffset, final byte[] fragment) {
            final UnsafeBuffer payload = new UnsafeBuffer(new byte[28 + fragment.length]);
            chunk.wrapAndApplyHeader(payload, 0, header).bootstrapId(bootstrapId).totalLength(totalLength).chunkOffset(chunkOffset)
                .putFragment(new UnsafeBuffer(fragment), 0, fragment.length);
            return proposal(sourceId, configuration, clock, payload);
        }

        UnsafeBuffer proposal(final int sourceId, final int configuration, final int clock, final UnsafeBuffer payload) {
            final UnsafeBuffer input = new UnsafeBuffer(new byte[Wire.PROPOSAL_OVERHEAD + payload.capacity()]);
            wire.encodeProposal(input, 0, LOG, sourceId + 10L, ++submitted[sourceId], sourceId, configuration, clock,
                10, 11, 12, payload, 0, payload.capacity());
            return input;
        }
    }
}
