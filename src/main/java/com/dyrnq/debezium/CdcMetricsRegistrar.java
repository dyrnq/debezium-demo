package com.dyrnq.debezium;

import com.dyrnq.debezium.cdc.MysqlSinkProcessor;
import com.dyrnq.debezium.cdc.RingBufferDispatcher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * Registers CDC pipeline metrics with Micrometer for monitoring and alerting.
 *
 * <p>Exposed metrics (all under the {@code cdc.*} namespace):</p>
 * <ul>
 *   <li>{@code cdc.events.processed} — counter of successfully processed events</li>
 *   <li>{@code cdc.events.errors} — counter of failed event processing attempts</li>
 *   <li>{@code cdc.ring.buffer.usage} — ring buffer fill ratio (0.0–1.0)</li>
 *   <li>{@code cdc.ring.dropped} — counter of events dropped due to ring buffer full</li>
 *   <li>{@code cdc.leader.fencing.token} — current fencing epoch (monotonically increasing)</li>
 *   <li>{@code cdc.leader.active} — 1 if this instance is the leader, 0 if standby</li>
 * </ul>
 */
@Component
public class CdcMetricsRegistrar {

    private final MeterRegistry registry;
    private final MysqlSinkProcessor sinkProcessor;
    private final RingBufferDispatcher ringBuffer;
    private final RedisLeaderElection leaderElection;

    public CdcMetricsRegistrar(MeterRegistry registry,
                               MysqlSinkProcessor sinkProcessor,
                               RingBufferDispatcher ringBuffer,
                               RedisLeaderElection leaderElection) {
        this.registry = registry;
        this.sinkProcessor = sinkProcessor;
        this.ringBuffer = ringBuffer;
        this.leaderElection = leaderElection;
    }

    @PostConstruct
    public void registerMetrics() {
        // Counters (derived from AtomicLong inside the components)
        Gauge.builder("cdc.events.processed", sinkProcessor, MysqlSinkProcessor::getProcessedCount)
                .description("Total CDC events successfully written to sink")
                .register(registry);

        Gauge.builder("cdc.events.errors", sinkProcessor, MysqlSinkProcessor::getErrorCount)
                .description("Total CDC event processing errors")
                .register(registry);

        Gauge.builder("cdc.ring.buffer.usage", ringBuffer, RingBufferDispatcher::capacityUsage)
                .description("Ring buffer fill ratio (0.0 empty to 1.0 full)")
                .register(registry);

        Gauge.builder("cdc.ring.buffer.size", ringBuffer, r -> r.size())
                .description("Current number of events in ring buffer")
                .register(registry);

        Gauge.builder("cdc.ring.dropped", ringBuffer, RingBufferDispatcher::droppedCount)
                .description("Events dropped due to ring buffer full")
                .register(registry);

        Gauge.builder("cdc.leader.fencing.token", leaderElection, RedisLeaderElection::getFencingToken)
                .description("Current fencing epoch — higher = more recent leadership")
                .register(registry);

        Gauge.builder("cdc.leader.active", leaderElection, le -> le.isLeader() ? 1.0 : 0.0)
                .description("1 if this instance is the leader, 0 if standby")
                .register(registry);
    }
}
