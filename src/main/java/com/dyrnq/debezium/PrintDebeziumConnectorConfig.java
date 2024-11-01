package com.dyrnq.debezium;

import io.debezium.connector.mysql.MySqlConnectorConfig;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class PrintDebeziumConnectorConfig {

    public static void main(String[] args) throws Exception {


        java.util.Set<String> set = MySqlConnectorConfig.ALL_FIELDS.allFieldNames();
        Set<String> sortedSet = set.stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String s : sortedSet) {
            System.out.println(s);
        }


        //System.out.println(JdbcSinkConnectorConfig.ALL_FIELDS.allFieldNames());


    }
}
