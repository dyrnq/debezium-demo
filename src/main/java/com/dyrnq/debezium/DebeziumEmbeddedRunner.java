package com.dyrnq.debezium;

import com.dyrnq.debezium.cdc.CdcEventParser;
import com.dyrnq.debezium.cdc.RingBufferDispatcher;
import com.dyrnq.debezium.model.CdcEvent;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.util.LoggingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.beans.factory.annotation.Value;
import com.dyrnq.debezium.cdc.MysqlSinkWriter;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Slf4j
public class DebeziumEmbeddedRunner implements ApplicationRunner, ApplicationListener<ContextClosedEvent> {

    private final Configuration config;
    private final RedisLeaderElection leaderElection;
    private final RingBufferDispatcher dispatcher;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final int ringStallTimeoutMs;
    private final MysqlSinkWriter snapshotWriter;
    private final AtomicBoolean ringFull = new AtomicBoolean(false);

    private volatile DebeziumEngine<ChangeEvent<String, String>> engine;
    private volatile ExecutorService executor;

    public DebeziumEmbeddedRunner(Configuration config,
                                  RedisLeaderElection leaderElection,
                                  RingBufferDispatcher dispatcher,
                                  @Value("${debezium.ring.stall-timeout-ms:25000}") int ringStallTimeoutMs,
                                  MysqlSinkWriter snapshotWriter) {
        this.config = config;
        this.leaderElection = leaderElection;
        this.dispatcher = dispatcher;
        this.ringStallTimeoutMs = ringStallTimeoutMs;
        this.snapshotWriter = snapshotWriter;
    }

    @Override
    public void run(ApplicationArguments args) {
        try {
            while (shutdownLatch.getCount() > 0) {
                while (!leaderElection.tryAcquire()) {
                    log.debug("Standby — waiting for leadership...");
                    if (shutdownLatch.await(5, TimeUnit.SECONDS)) return;
                }

                log.info("Becoming leader ({}), starting Debezium engine...",
                        leaderElection.getInstanceId());
                ringFull.set(false);
                startEngine();
                leaderElection.startRenewal();

                while (leaderElection.isLeader() && shutdownLatch.getCount() > 0) {
                    if (ringFull.get()) {
                        log.warn("Ring full — downstream appears dead, "
                                + "suspending engine immediately to prevent data loss");
                        abortEngine(); // ← force-kill: no flush, offset stays safe
                        break;
                    }
                    if (shutdownLatch.await(1, TimeUnit.SECONDS)) break;
                }

                if (!ringFull.get()) {
                    stopEngine(); // normal shutdown: flush offset
                }
                leaderElection.stopRenewal();
                leaderElection.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            stopEngine();
            leaderElection.release();
        }
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        log.info("Shutdown signal received");
        shutdownLatch.countDown();
    }

    @Override
    public boolean supportsAsyncExecution() {
        return false;
    }

    private void startEngine() {
        CountDownLatch firstLatch = new CountDownLatch(1);

        engine = DebeziumEngine.create(Json.class)
                .notifying(record -> {
                    String value = record.value();
                    if (value == null) return;
                    if (ringFull.get()) return;

                    // ── Fencing check before processing each event ──
                    // If the lock expired (e.g. long GC pause) and another instance
                    // took over, this check detects it immediately and stops
                    // producing into the ring buffer, preventing dual-write.
                    if (!leaderElection.isValidLeader()) {
                        log.warn("Fencing check failed in event callback — stopping engine");
                        ringFull.set(true); // trigger abort in main loop
                        return;
                    }

                    try {
                        CdcEvent event = CdcEventParser.parse(record.key(), value);
                        if (event == null) return;

                        // ── Gradual backpressure ──
                        // Instead of binary (ring ok → kill engine), apply progressive
                        // slowdown when the ring buffer fills up. This gives the sink
                        // consumer time to drain before we resort to killing the engine.
                        double usage = dispatcher.capacityUsage();
                        if (usage > 0.90) {
                            // >90% full: heavy throttle — 100ms pause per event
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                            if (dispatcher.capacityUsage() > 0.95) {
                                log.warn("Ring buffer critically full ({}%) — heavy throttle",
                                        (int)(usage * 100));
                            }
                        } else if (usage > 0.70) {
                            // >70% full: light throttle — 10ms pause per event
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                        }

                        long deadline = System.currentTimeMillis() + ringStallTimeoutMs;
                        while (!dispatcher.offer(event)) {
                            if (System.currentTimeMillis() >= deadline) {
                                ringFull.set(true);
                                log.error("Ring stalled >{}ms — event {}.{} abandoned, "
                                        + "leader will abort engine (no offset flush)",
                                        ringStallTimeoutMs,
                                        event.getDatabase(), event.getTable());
                                return;
                            }
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
                        }
                    } catch (Exception e) {
                        log.error("Failed to parse/dispatch CDC event", e);
                    }
                })
                .using(this.getClass().getClassLoader())
                .using(new DebeziumEngine.ConnectorCallback() {
                    @Override public void connectorStarted() { log.info("Connector started"); }
                    @Override public void connectorStopped() { log.info("Connector stopped"); }
                })
                .using((success, message, error) -> {
                    if (error != null) log.error("Engine shutdown error", error);
                    firstLatch.countDown();
                })
                .build();

        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "debezium-engine");
            t.setDaemon(false);
            return t;
        });
        executor.submit(() -> {
            LoggingContext.forConnector("DebeziumEmbeddedRunner", "", "engine");
            engine.run();
        });

        try {
            if (!firstLatch.await(30, TimeUnit.SECONDS)) {
                log.warn("Engine init not confirmed within 30s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Normal shutdown: close engine gracefully → offset flushed to Redis.
     */
    private void stopEngine() {
        if (engine != null) {
            // Close on a separate thread with timeout — stuck MySQL connection
            // must not block JVM shutdown.
            DebeziumEngine<ChangeEvent<String, String>> eng = engine;
            engine = null;
            Future<?> closeFuture = Executors.newSingleThreadExecutor().submit(() -> {
                try { eng.close(); } catch (IOException e) { /* ignore */ }
            });
            try {
                closeFuture.get(20, TimeUnit.SECONDS);
                log.info("Debezium engine closed (offset flushed)");
            } catch (Exception e) {
                log.error("Engine close timed out or failed — offset may not be flushed", e);
                closeFuture.cancel(true);
            }
        }
        shutdownExecutor(false);
    }

    /**
     * Abort engine when ring is full: force-interrupt WITHOUT calling close().
     * The in-memory offset is discarded — Redis offset stays at the last safe
     * position. On restart, Debezium replays events from that point.
     */
    private void abortEngine() {
        if (executor != null && !executor.isShutdown()) {
            log.warn("Aborting engine — interrupting thread (offset NOT flushed)");
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error("Engine thread did not terminate within 10s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            executor = null;
            engine = null;
        }
    }

    private void shutdownExecutor(boolean interrupt) {
        if (executor != null && !executor.isShutdown()) {
            if (interrupt) executor.shutdownNow(); else executor.shutdown();
            try {
                if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        log.error("Executor refused to terminate");
                    }
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            executor = null;
        }
    }
}
