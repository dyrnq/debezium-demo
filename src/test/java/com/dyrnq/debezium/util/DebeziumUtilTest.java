package com.dyrnq.debezium.util;


import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;


public class DebeziumUtilTest {

    @Test
    public void printOffsetData() throws IOException {
        DebeziumUtil.jsonOffsetDataFile(new File("data/offset.data"));
    }

    @Test
    public void defaultMySqlConnectorConfig() throws IOException {
        DebeziumUtil.defaultMySqlConnectorConfig();
    }
    @Test
    public void jdbcTypes() throws IOException {
        DebeziumUtil.jdbcTypes();
    }
}
