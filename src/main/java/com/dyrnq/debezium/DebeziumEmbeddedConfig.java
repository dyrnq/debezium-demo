package com.dyrnq.debezium;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Objects;
import java.util.Properties;

@Configuration
@ConfigurationProperties(prefix = "debezium")
public class DebeziumEmbeddedConfig {

    Properties configProperties;

    public Properties getConfigProperties() {
        return configProperties;
    }

    public void setConfigProperties(Properties configProperties) {
        this.configProperties = configProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DebeziumEmbeddedConfig other)) return false;
        return Objects.equals(getConfigProperties(), other.getConfigProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConfigProperties());
    }

    @Override
    public String toString() {
        return "DebeziumEmbeddedConfig(configProperties=" + getConfigProperties() + ")";
    }

    @Bean
    io.debezium.config.Configuration debeziumConfig() {
        return io.debezium.config.Configuration.from(configProperties);
    }
}
