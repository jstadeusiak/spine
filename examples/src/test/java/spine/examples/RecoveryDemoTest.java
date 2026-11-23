package spine.examples;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RecoveryDemoTest {
    @Test
    void replaysOneEventFromTheEmptyCheckpoint() throws IOException {
        RecoveryDemo.main(new String[]{"--events", "1", "--payload-bytes", "20"});
    }

    @Test
    void rejectsAnOptionWithoutItsValue() {
        assertThrows(IllegalArgumentException.class, () -> RecoveryDemo.main(new String[]{"--events"}));
    }
}
