package com.dyrnq.debezium;

import com.dyrnq.debezium.cdc.MysqlSinkProcessor;
import com.dyrnq.debezium.cdc.RingBufferDispatcher;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class DebeziumHealthIndicator implements HealthIndicator {

    private final RedisLeaderElection leaderElection;
    private final RingBufferDispatcher ringBuffer;
    private final MysqlSinkProcessor sinkProcessor;

    public DebeziumHealthIndicator(RedisLeaderElection leaderElection,
                                   RingBufferDispatcher ringBuffer,
                                   MysqlSinkProcessor sinkProcessor) {
        this.leaderElection = leaderElection;
        this.ringBuffer = ringBuffer;
        this.sinkProcessor = sinkProcessor;
    }

    @Override
    public Health health() {
        return Health.up()
                .withDetail("role", leaderElection.isLeader() ? "leader" : "standby")
                .withDetail("instance", leaderElection.getInstanceId())
                .withDetail("fencingToken", leaderElection.getFencingToken())
                .withDetail("ringBuffer", ringBuffer.size())
                .withDetail("dropped", ringBuffer.droppedCount())
                .withDetail("processed", sinkProcessor.getProcessedCount())
                .withDetail("errors", sinkProcessor.getErrorCount())
                .build();
    }
}
