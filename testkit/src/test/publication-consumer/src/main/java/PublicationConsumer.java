import io.github.jstadeusiak.spine.client.ReliableProducer;
import io.github.jstadeusiak.spine.protocol.Wire;
import io.github.jstadeusiak.spine.sequencer.SequencerCore;
import io.github.jstadeusiak.spine.testkit.ApplicationConformance;
import io.github.jstadeusiak.spine.testkit.CounterApplication;
import io.github.jstadeusiak.spine.testkit.CounterExample;

public final class PublicationConsumer {
    public static void main(String[] args) {
        for (Class<?> type : new Class<?>[]{Wire.class, ReliableProducer.class, SequencerCore.class,
            ApplicationConformance.class, CounterApplication.class}) {
            String source = type.getProtectionDomain().getCodeSource().getLocation().toString();
            if (!source.endsWith(".jar")) throw new AssertionError("Not a published jar: " + source);
            System.out.println(type.getName() + " <- " + source);
        }
        if (PublicationConsumer.class.getClassLoader().getResource(
            "io/github/jstadeusiak/spine/protocol/market/sbe/BookResultDecoder.class") != null) {
            throw new AssertionError("Generic published application contains market codecs");
        }
        ApplicationConformance.Report report = ApplicationConformance.verify(
            CounterApplication::new, CounterApplication.IDENTITY, CounterExample.LOG_ID, CounterExample.corpus());
        if (!report.canonicalSha256().equals("02a96529abded055c7448fe0efc71fe6e9e64d1c2b00fceef371ad3387913f58")
            || !report.finalSnapshotSha256().equals("cce5529d571e1c3ca62cd15bb9221f954d8faeee2d9ad617c02b60ba5d550b64")) {
            throw new AssertionError("Published runtime differs from independent counter oracle");
        }
        System.out.println("PUBLISHED_GENERIC_CONSUMER_PASSED " + report);
    }
}
