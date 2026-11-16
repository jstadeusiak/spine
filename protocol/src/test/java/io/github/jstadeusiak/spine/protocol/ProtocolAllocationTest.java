package io.github.jstadeusiak.spine.protocol;

import com.sun.management.ThreadMXBean;
import io.github.jstadeusiak.spine.protocol.sbe.AdmissionStatus;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolAllocationTest {
    private static volatile Object escaped;
    private static volatile long observed;
    private final Wire wire = new Wire();
    private final CaptureCodec capture = new CaptureCodec();
    private final UnsafeBuffer raw = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES - CaptureCodec.OBSERVATION_OVERHEAD]);
    private final UnsafeBuffer payload = new UnsafeBuffer(new byte[Wire.MAX_PAYLOAD_BYTES]);
    private final UnsafeBuffer proposal = new UnsafeBuffer(new byte[Wire.MAX_PROPOSAL_BYTES]);
    private final UnsafeBuffer event = new UnsafeBuffer(new byte[Wire.MAX_EVENT_BYTES]);
    private final UnsafeBuffer result = new UnsafeBuffer(new byte[Wire.MAX_RESULT_BYTES]);
    private final UnsafeBuffer malformed = new UnsafeBuffer(new byte[8]);

    @Test void warmedPrimitiveEncodeDecodeAndMalformedRejectionHaveNoAllocationSlope() {
        assertThat(ManagementFactory.getThreadMXBean()).isInstanceOf(ThreadMXBean.class);
        final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assertThat(bean.isThreadAllocatedMemorySupported()).isTrue();
        bean.setThreadAllocatedMemoryEnabled(true);
        assertThat(bean.isThreadAllocatedMemoryEnabled()).isTrue();
        final long thread = Thread.currentThread().threadId();
        final long beforeControl = bean.getThreadAllocatedBytes(thread);
        for (int i = 0; i < 1000; i++) escaped = new byte[32];
        final long afterControl = bean.getThreadAllocatedBytes(thread);
        assertThat(beforeControl).isNotNegative();
        assertThat(afterControl - beforeControl).isGreaterThanOrEqualTo(32000);
        exercise(100000);
        final long beforeShort = bean.getThreadAllocatedBytes(thread);
        exercise(10000);
        final long afterShort = bean.getThreadAllocatedBytes(thread);
        final long beforeLong = bean.getThreadAllocatedBytes(thread);
        exercise(100000);
        final long afterLong = bean.getThreadAllocatedBytes(thread);
        assertThat(beforeShort).isNotNegative();
        assertThat(beforeLong).isNotNegative();
        assertThat(afterShort - beforeShort).isZero();
        assertThat(afterLong - beforeLong).isZero();
    }

    private void exercise(final int count) {
        long checksum = 0;
        for (int i = 0; i < count; i++) {
            final int rawLength = (i & 1023) == 0 ? raw.capacity() : 32;
            final int payloadLength = capture.encodeObservation(payload, 0, i + 1L, 0, i,
                null, 0, 0, raw, 0, rawLength);
            final int proposalLength = wire.encodeProposal(proposal, 0, 1, 2, i + 1L, Integer.MIN_VALUE, 1, 1,
                -1, -1, -1, payload, 0, payloadLength);
            final int eventLength = wire.encodeEvent(event, 0, 1, i + 1L, i, Integer.MIN_VALUE, 1, 1,
                -1, -1, -1, payload, 0, payloadLength);
            final int resultLength = wire.encodeResult(result, 0, 2, i + 1L, AdmissionStatus.ACCEPTED, event, 0, eventLength);
            if (!wire.validProposal(proposal, 0, proposalLength) || !wire.validResult(result, 0, resultLength)
                || !capture.validObservation(payload, 0, payloadLength) || wire.validProposal(malformed, 0, 8)) {
                throw new AssertionError("unexpected codec result");
            }
            checksum += wire.event.sequence() + capture.observation.rawFrameSequence();
            result.putByte(24, (byte) 255);
            if (wire.validResult(result, 0, resultLength)) throw new AssertionError("malformed status accepted");
            checksum ^= result.getByte(resultLength - 1);
        }
        observed = checksum;
    }
}
