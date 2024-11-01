package com.dyrnq.debezium;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Data
@Configuration
@Slf4j
@ConfigurationProperties(prefix = "debezium")
public class DebeziumEmbeddedConfig {

    Properties configProperties;

    @Bean
    io.debezium.config.Configuration debeziumConfig() {
        return io.debezium.config.Configuration.from(configProperties);
    }
}
