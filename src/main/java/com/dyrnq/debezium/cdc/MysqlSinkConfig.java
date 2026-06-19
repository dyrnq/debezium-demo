package com.dyrnq.debezium.cdc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class MysqlSinkConfig {

    @Bean
    @ConfigurationProperties(prefix = "debezium.sink.datasource")
    public HikariConfig sinkHikariConfig() {
        HikariConfig cfg = new HikariConfig();
        cfg.setDriverClassName("com.mysql.cj.jdbc.Driver");
        cfg.setMaximumPoolSize(5);
        cfg.setMinimumIdle(1);
        cfg.setConnectionTimeout(5000);
        cfg.setAutoCommit(true);
        return cfg;
    }

    @Bean
    public DataSource sinkDataSource(HikariConfig sinkHikariConfig) {
        return new HikariDataSource(sinkHikariConfig);
    }

    @Bean
    public JdbcTemplate sinkJdbcTemplate(DataSource sinkDataSource) {
        return new JdbcTemplate(sinkDataSource);
    }
}
