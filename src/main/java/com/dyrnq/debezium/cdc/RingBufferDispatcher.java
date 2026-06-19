package com.dyrnq.debezium.cdc;

import com.dyrnq.debezium.model.CdcEvent;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class RingBufferDispatcher {

    private final Disruptor<CdcEventHolder> disruptor;
    private final RingBuffer<CdcEventHolder> ringBuffer;
    private final SequenceBarrier barrier;
    private final AtomicLong droppedCount = new AtomicLong(0);
    private volatile long cursor = -1;

    public RingBufferDispatcher(
            @Value("${debezium.ring.buffer-size:8192}") int bufferSize) {
        this.disruptor = new Disruptor<>(
                CdcEventHolder::new, bufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.SINGLE, new SleepingWaitStrategy()
        );
        this.ringBuffer = disruptor.getRingBuffer();
        this.barrier = ringBuffer.newBarrier();
        this.disruptor.start();
        log.info("Disruptor ring buffer started: bufferSize={}", bufferSize);
    }

    public boolean offer(CdcEvent event) {
        if (ringBuffer.remainingCapacity() == 0) {
            droppedCount.incrementAndGet();
            return false;
        }
        long seq = ringBuffer.next();
        ringBuffer.get(seq).event = event;
        ringBuffer.publish(seq);
        return true;
    }

    public List<CdcEvent> drainBatch(int maxSize, long timeoutMs) {
        List<CdcEvent> batch = new ArrayList<>(Math.min(maxSize, 256));
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (batch.size() < maxSize) {
            long next = cursor + 1;
            try {
                long available = barrier.waitFor(next);
                for (long i = next; i <= available && batch.size() < maxSize; i++) {
                    CdcEvent e = ringBuffer.get(i).event;
                    if (e != null) { batch.add(e); ringBuffer.get(i).event = null; }
                }
                cursor = Math.max(cursor, available);
                if (batch.size() >= maxSize || System.currentTimeMillis() >= deadline) break;
            } catch (com.lmax.disruptor.TimeoutException e) {
                break;
            } catch (Exception e) {
                log.error("Drain error", e); break;
            }
        }
        return batch;
    }

    public int size()              { return (int)(ringBuffer.getCursor() - cursor); }

    // /u2014 Backpressure /u2014/

    // Buffer fill ratio for backpressure decisions.
    //  0.0 = empty, 1.0 = full.
    public double capacityUsage() {
        int bufferSize = ringBuffer.getBufferSize();
        long remaining = ringBuffer.remainingCapacity();
        return 1.0 - ((double) remaining / bufferSize);
    }

    public int bufferSize() { return ringBuffer.getBufferSize(); }
    public long droppedCount()     { return droppedCount.get(); }

    static class CdcEventHolder { CdcEvent event; }
}
