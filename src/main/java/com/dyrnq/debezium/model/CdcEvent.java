package com.dyrnq.debezium.model;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.util.List;
import java.util.Map;

@Data
@Builder
public class CdcEvent {
    private String op;
    private String database;
    private String table;
    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> keys;
    private Long tsMs;

    // DDL fields (op="m")
    private String ddl;
    @Singular
    private List<TableChange> tableChanges;

    public String targetTable() {
        return database + "_" + table;
    }

    public boolean isInsert()   { return "c".equals(op); }
    public boolean isSnapshot() { return "r".equals(op); }
    public boolean isUpdate()   { return "u".equals(op); }
    public boolean isDelete()   { return "d".equals(op); }
    public boolean isDdl()      { return "m".equals(op); }
    public boolean isTruncate() { return "t".equals(op); }

    public Map<String, Object> data() {
        return isDelete() ? before : after;
    }

    @Data
    @Builder
    public static class TableChange {
        private String type;              // CREATE, ALTER, DROP
        private String id;                // "mytest2"."stuff"
        private Table table;
    }

    @Data
    @Builder
    public static class Table {
        private String defaultCharsetName;
        @Singular
        private List<String> primaryKeyColumnNames;
        @Singular
        private List<Column> columns;
    }

    @Data
    @Builder
    public static class Column {
        private String name;
        private int jdbcType;
        private String typeName;
        private int length;
        private int scale;
        private boolean optional;
    }
}
