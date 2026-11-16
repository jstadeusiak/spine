import io.github.jstadeusiak.spine.examples.binance.BinanceApplication;
import io.github.jstadeusiak.spine.protocol.market.sbe.BookResultDecoder;
import io.github.jstadeusiak.spine.sequencer.SequencerCore;
import org.agrona.concurrent.UnsafeBuffer;
import java.util.Arrays;

public final class OptionalConsumer {
    public static void main(String[] args) {
        for (Class<?> type : new Class<?>[]{BinanceApplication.class, BookResultDecoder.class}) {
            String source = type.getProtectionDomain().getCodeSource().getLocation().toString();
            if (!source.contains("examples-binance-") || !source.endsWith(".jar")) {
                throw new AssertionError("Optional application not loaded from its published jar: " + source);
            }
        }
        SequencerCore original = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        byte[] snapshot = new byte[original.snapshot()];
        original.resultBuffer().getBytes(0, snapshot);
        SequencerCore restored = new SequencerCore(BinanceApplication.IDENTITY, new BinanceApplication());
        restored.restore(new UnsafeBuffer(snapshot), 0, snapshot.length);
        byte[] actual = new byte[restored.snapshot()];
        restored.resultBuffer().getBytes(0, actual);
        if (!Arrays.equals(snapshot, actual)) throw new AssertionError("Optional application snapshot differs");
        System.out.println("PUBLISHED_OPTIONAL_BINANCE_PASSED snapshotBytes=" + actual.length);
    }
}
