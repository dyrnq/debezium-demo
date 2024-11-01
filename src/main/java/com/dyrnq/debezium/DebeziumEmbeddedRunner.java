package com.dyrnq.debezium;

import io.debezium.embedded.Connect;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.format.Json;
import io.debezium.util.LoggingContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class DebeziumEmbeddedRunner implements ApplicationRunner, ApplicationListener<ContextClosedEvent> {
    private final io.debezium.config.Configuration config;

    private static void shutdownHook(EmbeddedEngine engine) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Requesting embedded engine to shut down");
            engine.stop();
        }));
    }

    private static void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10L, TimeUnit.SECONDS)) {
                log.debug("Waiting another 10 seconds for the embedded engine to shut down");
            }
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        CountDownLatch firstLatch = new CountDownLatch(1);
        try (DebeziumEngine engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))

                .using(config.asProperties())
                .notifying((records, committer) -> {

                    for (RecordChangeEvent<SourceRecord> record : records) {
                        log.info("record: {} ", record);
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .using(this.getClass().getClassLoader())
                .using((success, message, error) -> {
                    if (error != null) {
                        log.error("Error while shutting down", error);
                    }
                    firstLatch.countDown();
                })
                .build();
        ) {
            // Run the engine asynchronously ...
            ExecutorService exec = Executors.newFixedThreadPool(1);
            exec.execute(() -> {
                LoggingContext.forConnector(DebeziumEmbeddedRunner.class.getSimpleName(), "", "engine");
                engine.run();
            });

            firstLatch.await(5000, TimeUnit.MILLISECONDS);
            //shutdownHook(engine);
            awaitTermination(exec);
            // Do something else or wait for a signal or an event
        }

    }


    @Override
    public void onApplicationEvent(ContextClosedEvent event) {

    }

    @Override
    public boolean supportsAsyncExecution() {
        return ApplicationListener.super.supportsAsyncExecution();
    }
}
