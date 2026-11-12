package com.dyrnq.debezium.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CDC event model.
 *
 * <p>Was previously annotated with Lombok's {@code @Data}, {@code @Builder} and {@code @Singular};
 * the API those annotations generated is reproduced by hand here so the project no longer needs
 * Lombok on the classpath. The builder keeps Lombok's exact semantics: singular collections are
 * appended to (never replaced), reject {@code null}, and are frozen into an immutable list by
 * {@code build()}.
 */
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
    private List<TableChange> tableChanges;

    CdcEvent(String op, String database, String table, Map<String, Object> before,
             Map<String, Object> after, Map<String, Object> keys, Long tsMs, String ddl,
             List<TableChange> tableChanges) {
        this.op = op;
        this.database = database;
        this.table = table;
        this.before = before;
        this.after = after;
        this.keys = keys;
        this.tsMs = tsMs;
        this.ddl = ddl;
        this.tableChanges = tableChanges;
    }

    public static CdcEventBuilder builder() {
        return new CdcEventBuilder();
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public Map<String, Object> getKeys() {
        return keys;
    }

    public void setKeys(Map<String, Object> keys) {
        this.keys = keys;
    }

    public Long getTsMs() {
        return tsMs;
    }

    public void setTsMs(Long tsMs) {
        this.tsMs = tsMs;
    }

    public String getDdl() {
        return ddl;
    }

    public void setDdl(String ddl) {
        this.ddl = ddl;
    }

    public List<TableChange> getTableChanges() {
        return tableChanges;
    }

    public void setTableChanges(List<TableChange> tableChanges) {
        this.tableChanges = tableChanges;
    }

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

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof CdcEvent other)) return false;
        if (!other.canEqual(this)) return false;
        return Objects.equals(getOp(), other.getOp())
                && Objects.equals(getDatabase(), other.getDatabase())
                && Objects.equals(getTable(), other.getTable())
                && Objects.equals(getBefore(), other.getBefore())
                && Objects.equals(getAfter(), other.getAfter())
                && Objects.equals(getKeys(), other.getKeys())
                && Objects.equals(getTsMs(), other.getTsMs())
                && Objects.equals(getDdl(), other.getDdl())
                && Objects.equals(getTableChanges(), other.getTableChanges());
    }

    protected boolean canEqual(Object other) {
        return other instanceof CdcEvent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOp(), getDatabase(), getTable(), getBefore(), getAfter(),
                getKeys(), getTsMs(), getDdl(), getTableChanges());
    }

    @Override
    public String toString() {
        return "CdcEvent(op=" + getOp() + ", database=" + getDatabase() + ", table=" + getTable()
                + ", before=" + getBefore() + ", after=" + getAfter() + ", keys=" + getKeys()
                + ", tsMs=" + getTsMs() + ", ddl=" + getDdl()
                + ", tableChanges=" + getTableChanges() + ")";
    }

    /**
     * Freezes a builder-held collection the way Lombok's {@code @Singular} does: an empty list for
     * no elements, a singleton list for one, and an unmodifiable copy for anything more.
     */
    private static <T> List<T> frozen(List<T> elements) {
        if (elements == null || elements.isEmpty()) {
            return Collections.emptyList();
        }
        if (elements.size() == 1) {
            return Collections.singletonList(elements.get(0));
        }
        return Collections.unmodifiableList(new ArrayList<>(elements));
    }

    public static class CdcEventBuilder {
        private String op;
        private String database;
        private String table;
        private Map<String, Object> before;
        private Map<String, Object> after;
        private Map<String, Object> keys;
        private Long tsMs;
        private String ddl;
        private ArrayList<TableChange> tableChanges;

        CdcEventBuilder() {
        }

        public CdcEventBuilder op(String op) {
            this.op = op;
            return this;
        }

        public CdcEventBuilder database(String database) {
            this.database = database;
            return this;
        }

        public CdcEventBuilder table(String table) {
            this.table = table;
            return this;
        }

        public CdcEventBuilder before(Map<String, Object> before) {
            this.before = before;
            return this;
        }

        public CdcEventBuilder after(Map<String, Object> after) {
            this.after = after;
            return this;
        }

        public CdcEventBuilder keys(Map<String, Object> keys) {
            this.keys = keys;
            return this;
        }

        public CdcEventBuilder tsMs(Long tsMs) {
            this.tsMs = tsMs;
            return this;
        }

        public CdcEventBuilder ddl(String ddl) {
            this.ddl = ddl;
            return this;
        }

        public CdcEventBuilder tableChange(TableChange tableChange) {
            if (this.tableChanges == null) {
                this.tableChanges = new ArrayList<>();
            }
            this.tableChanges.add(tableChange);
            return this;
        }

        public CdcEventBuilder tableChanges(Collection<? extends TableChange> tableChanges) {
            if (tableChanges == null) {
                throw new NullPointerException("tableChanges cannot be null");
            }
            if (this.tableChanges == null) {
                this.tableChanges = new ArrayList<>();
            }
            this.tableChanges.addAll(tableChanges);
            return this;
        }

        public CdcEventBuilder clearTableChanges() {
            if (this.tableChanges != null) {
                this.tableChanges.clear();
            }
            return this;
        }

        public CdcEvent build() {
            return new CdcEvent(op, database, table, before, after, keys, tsMs, ddl, frozen(tableChanges));
        }

        @Override
        public String toString() {
            return "CdcEvent.CdcEventBuilder(op=" + this.op + ", database=" + this.database
                    + ", table=" + this.table + ", before=" + this.before + ", after=" + this.after
                    + ", keys=" + this.keys + ", tsMs=" + this.tsMs + ", ddl=" + this.ddl
                    + ", tableChanges=" + this.tableChanges + ")";
        }
    }

    public static class TableChange {
        private String type;              // CREATE, ALTER, DROP
        private String id;                // "mytest2"."stuff"
        private Table table;

        TableChange(String type, String id, Table table) {
            this.type = type;
            this.id = id;
            this.table = table;
        }

        public static TableChangeBuilder builder() {
            return new TableChangeBuilder();
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Table getTable() {
            return table;
        }

        public void setTable(Table table) {
            this.table = table;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof TableChange other)) return false;
            if (!other.canEqual(this)) return false;
            return Objects.equals(getType(), other.getType())
                    && Objects.equals(getId(), other.getId())
                    && Objects.equals(getTable(), other.getTable());
        }

        protected boolean canEqual(Object other) {
            return other instanceof TableChange;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getType(), getId(), getTable());
        }

        @Override
        public String toString() {
            return "CdcEvent.TableChange(type=" + getType() + ", id=" + getId()
                    + ", table=" + getTable() + ")";
        }

        public static class TableChangeBuilder {
            private String type;
            private String id;
            private Table table;

            TableChangeBuilder() {
            }

            public TableChangeBuilder type(String type) {
                this.type = type;
                return this;
            }

            public TableChangeBuilder id(String id) {
                this.id = id;
                return this;
            }

            public TableChangeBuilder table(Table table) {
                this.table = table;
                return this;
            }

            public TableChange build() {
                return new TableChange(type, id, table);
            }

            @Override
            public String toString() {
                return "CdcEvent.TableChange.TableChangeBuilder(type=" + this.type
                        + ", id=" + this.id + ", table=" + this.table + ")";
            }
        }
    }

    public static class Table {
        private String defaultCharsetName;
        private List<String> primaryKeyColumnNames;
        private List<Column> columns;

        Table(String defaultCharsetName, List<String> primaryKeyColumnNames, List<Column> columns) {
            this.defaultCharsetName = defaultCharsetName;
            this.primaryKeyColumnNames = primaryKeyColumnNames;
            this.columns = columns;
        }

        public static TableBuilder builder() {
            return new TableBuilder();
        }

        public String getDefaultCharsetName() {
            return defaultCharsetName;
        }

        public void setDefaultCharsetName(String defaultCharsetName) {
            this.defaultCharsetName = defaultCharsetName;
        }

        public List<String> getPrimaryKeyColumnNames() {
            return primaryKeyColumnNames;
        }

        public void setPrimaryKeyColumnNames(List<String> primaryKeyColumnNames) {
            this.primaryKeyColumnNames = primaryKeyColumnNames;
        }

        public List<Column> getColumns() {
            return columns;
        }

        public void setColumns(List<Column> columns) {
            this.columns = columns;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof Table other)) return false;
            if (!other.canEqual(this)) return false;
            return Objects.equals(getDefaultCharsetName(), other.getDefaultCharsetName())
                    && Objects.equals(getPrimaryKeyColumnNames(), other.getPrimaryKeyColumnNames())
                    && Objects.equals(getColumns(), other.getColumns());
        }

        protected boolean canEqual(Object other) {
            return other instanceof Table;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getDefaultCharsetName(), getPrimaryKeyColumnNames(), getColumns());
        }

        @Override
        public String toString() {
            return "CdcEvent.Table(defaultCharsetName=" + getDefaultCharsetName()
                    + ", primaryKeyColumnNames=" + getPrimaryKeyColumnNames()
                    + ", columns=" + getColumns() + ")";
        }

        public static class TableBuilder {
            private String defaultCharsetName;
            private ArrayList<String> primaryKeyColumnNames;
            private ArrayList<Column> columns;

            TableBuilder() {
            }

            public TableBuilder defaultCharsetName(String defaultCharsetName) {
                this.defaultCharsetName = defaultCharsetName;
                return this;
            }

            public TableBuilder primaryKeyColumnName(String primaryKeyColumnName) {
                if (this.primaryKeyColumnNames == null) {
                    this.primaryKeyColumnNames = new ArrayList<>();
                }
                this.primaryKeyColumnNames.add(primaryKeyColumnName);
                return this;
            }

            public TableBuilder primaryKeyColumnNames(Collection<? extends String> primaryKeyColumnNames) {
                if (primaryKeyColumnNames == null) {
                    throw new NullPointerException("primaryKeyColumnNames cannot be null");
                }
                if (this.primaryKeyColumnNames == null) {
                    this.primaryKeyColumnNames = new ArrayList<>();
                }
                this.primaryKeyColumnNames.addAll(primaryKeyColumnNames);
                return this;
            }

            public TableBuilder clearPrimaryKeyColumnNames() {
                if (this.primaryKeyColumnNames != null) {
                    this.primaryKeyColumnNames.clear();
                }
                return this;
            }

            public TableBuilder column(Column column) {
                if (this.columns == null) {
                    this.columns = new ArrayList<>();
                }
                this.columns.add(column);
                return this;
            }

            public TableBuilder columns(Collection<? extends Column> columns) {
                if (columns == null) {
                    throw new NullPointerException("columns cannot be null");
                }
                if (this.columns == null) {
                    this.columns = new ArrayList<>();
                }
                this.columns.addAll(columns);
                return this;
            }

            public TableBuilder clearColumns() {
                if (this.columns != null) {
                    this.columns.clear();
                }
                return this;
            }

            public Table build() {
                return new Table(defaultCharsetName, frozen(primaryKeyColumnNames), frozen(columns));
            }

            @Override
            public String toString() {
                return "CdcEvent.Table.TableBuilder(defaultCharsetName=" + this.defaultCharsetName
                        + ", primaryKeyColumnNames=" + this.primaryKeyColumnNames
                        + ", columns=" + this.columns + ")";
            }
        }
    }

    public static class Column {
        private String name;
        private int jdbcType;
        private String typeName;
        private int length;
        private int scale;
        private boolean optional;

        Column(String name, int jdbcType, String typeName, int length, int scale, boolean optional) {
            this.name = name;
            this.jdbcType = jdbcType;
            this.typeName = typeName;
            this.length = length;
            this.scale = scale;
            this.optional = optional;
        }

        public static ColumnBuilder builder() {
            return new ColumnBuilder();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getJdbcType() {
            return jdbcType;
        }

        public void setJdbcType(int jdbcType) {
            this.jdbcType = jdbcType;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public int getScale() {
            return scale;
        }

        public void setScale(int scale) {
            this.scale = scale;
        }

        public boolean isOptional() {
            return optional;
        }

        public void setOptional(boolean optional) {
            this.optional = optional;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof Column other)) return false;
            if (!other.canEqual(this)) return false;
            return getJdbcType() == other.getJdbcType()
                    && getLength() == other.getLength()
                    && getScale() == other.getScale()
                    && isOptional() == other.isOptional()
                    && Objects.equals(getName(), other.getName())
                    && Objects.equals(getTypeName(), other.getTypeName());
        }

        protected boolean canEqual(Object other) {
            return other instanceof Column;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getName(), getJdbcType(), getTypeName(), getLength(), getScale(), isOptional());
        }

        @Override
        public String toString() {
            return "CdcEvent.Column(name=" + getName() + ", jdbcType=" + getJdbcType()
                    + ", typeName=" + getTypeName() + ", length=" + getLength()
                    + ", scale=" + getScale() + ", optional=" + isOptional() + ")";
        }

        public static class ColumnBuilder {
            private String name;
            private int jdbcType;
            private String typeName;
            private int length;
            private int scale;
            private boolean optional;

            ColumnBuilder() {
            }

            public ColumnBuilder name(String name) {
                this.name = name;
                return this;
            }

            public ColumnBuilder jdbcType(int jdbcType) {
                this.jdbcType = jdbcType;
                return this;
            }

            public ColumnBuilder typeName(String typeName) {
                this.typeName = typeName;
                return this;
            }

            public ColumnBuilder length(int length) {
                this.length = length;
                return this;
            }

            public ColumnBuilder scale(int scale) {
                this.scale = scale;
                return this;
            }

            public ColumnBuilder optional(boolean optional) {
                this.optional = optional;
                return this;
            }

            public Column build() {
                return new Column(name, jdbcType, typeName, length, scale, optional);
            }

            @Override
            public String toString() {
                return "CdcEvent.Column.ColumnBuilder(name=" + this.name
                        + ", jdbcType=" + this.jdbcType + ", typeName=" + this.typeName
                        + ", length=" + this.length + ", scale=" + this.scale
                        + ", optional=" + this.optional + ")";
            }
        }
    }
}
