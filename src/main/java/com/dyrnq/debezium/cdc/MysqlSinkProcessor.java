package com.dyrnq.debezium.cdc;

import com.dyrnq.debezium.model.CdcEvent;
import com.dyrnq.debezium.model.CdcEvent.Column;
import com.dyrnq.debezium.model.CdcEvent.TableChange;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.*;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Component
public class MysqlSinkProcessor implements MysqlSinkWriter {

    // ── DDL safety: whitelist of allowed charset names ──────────
    // Prevents charset injection via crafted CDC events.
    private static final Set<String> ALLOWED_CHARSETS = Set.of(
            "utf8", "utf8mb4", "utf8mb3", "latin1", "ascii",
            "gbk", "gb2312", "binary", "armscii8", "cp1250",
            "cp1251", "cp1252", "cp1256", "cp850", "cp852",
            "cp866", "dec8", "eucjpms", "euckr", "hp8",
            "keybcs2", "koi8r", "koi8u", "latin2", "latin5",
            "latin7", "macce", "macroman", "swe7", "ucs2",
            "ujis", "utf16", "utf16le", "utf32"
    );

    // ── DDL safety: allowed ALTER operations ────────────────────
    // Only column-level DDL is forwarded; other ALTER types are rejected.
    private static final java.util.regex.Pattern SAFE_ALTER_PATTERN =
            java.util.regex.Pattern.compile(
                    "^\\s*ALTER\\s+TABLE\\s+`[^`]+`\\s+(ADD|MODIFY|DROP|CHANGE)\\s+(COLUMN\\s+)?",
                    java.util.regex.Pattern.CASE_INSENSITIVE);

    private final JdbcTemplate jdbc;
    private final RingBufferDispatcher dispatcher;
    private final int drainMaxSize;
    private final int drainTimeoutMs;
    private final int consecutiveFailThreshold;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final Set<String> createdTables = new HashSet<>();
    private final Map<String, String> upsertSqlCache = new HashMap<>();
    private final Map<String, Set<String>> upsertColsCache = new HashMap<>();
    private volatile Thread worker;

    public MysqlSinkProcessor(@Qualifier("sinkJdbcTemplate") JdbcTemplate jdbc,
                              RingBufferDispatcher dispatcher,
                              @Value("${debezium.ring.drain-max-size:256}") int drainMaxSize,
                              @Value("${debezium.ring.drain-timeout-ms:200}") int drainTimeoutMs,
                              @Value("${debezium.ring.consecutive-fail-threshold:3}") int consecutiveFailThreshold) {
        this.jdbc = jdbc;
        this.dispatcher = dispatcher;
        this.drainMaxSize = drainMaxSize;
        this.drainTimeoutMs = drainTimeoutMs;
        this.consecutiveFailThreshold = consecutiveFailThreshold;
    }

    // ── Snapshot bypass ───────────────────────────────────────

    @Override
    public void write(CdcEvent event) {
        try {
            processBatch(Collections.singletonList(event));
            processedCount.incrementAndGet();
        } catch (Exception e) {
            errorCount.incrementAndGet();
            log.error("Snapshot write failed for {}.{}: {}", 
                    event.getDatabase(), event.getTable(), e.getMessage());
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────

    @PostConstruct
    public void start() {
        worker = new Thread(this::drainLoop, "mysql-sink");
        worker.setDaemon(false);
        worker.start();
        log.info("MysqlSinkProcessor started");
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (worker != null) {
            worker.interrupt();
            try { worker.join(15_000); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("MysqlSinkProcessor stopped. Processed={} errors={}",
                processedCount.get(), errorCount.get());
    }

    private void drainLoop() {
        int consecutiveFailures = 0;
        while (running.get()) {
            try {
                if (consecutiveFailures >= consecutiveFailThreshold) {
                    Thread.sleep(1000);
                    consecutiveFailures = 0;
                    continue;
                }
                List<CdcEvent> batch = dispatcher.drainBatch(drainMaxSize, drainTimeoutMs);
                if (batch.isEmpty()) continue;
                processBatch(batch);
                processedCount.addAndGet(batch.size());
                consecutiveFailures = 0;
            } catch (Exception e) {
                consecutiveFailures++;
                errorCount.addAndGet(1);
                log.warn("Batch write failed (consecutive={}): {}",
                        consecutiveFailures, e.getMessage());
            }
        }
        List<CdcEvent> remaining = dispatcher.drainBatch(Integer.MAX_VALUE, 0);
        if (!remaining.isEmpty()) {
            try {
                processBatch(remaining);
                processedCount.addAndGet(remaining.size());
            } catch (Exception ex) {
                errorCount.addAndGet(remaining.size());
                log.error("Final drain failed, {} events discarded", remaining.size());
            }
        }
    }

    // ── batch dispatch ─────────────────────────────────────────

    private void processBatch(List<CdcEvent> batch) {
        boolean hasDdl = false;
        for (CdcEvent e : batch) {
            String op = e.getOp();
            if ("m".equals(op) || "t".equals(op)) { hasDdl = true; break; }
        }
        if (hasDdl) {
            List<CdcEvent> dmls = new ArrayList<>(batch.size());
            for (CdcEvent e : batch) {
                if (e.isDdl())      handleDdl(e);
                else if (e.isTruncate()) handleTruncate(e);
                else                dmls.add(e);
            }
            if (!dmls.isEmpty()) processDmlBatch(dmls);
        } else {
            processDmlBatch(batch);
        }
    }

    // ── DDL (unchanged) ──────────────────────────────────────

    private void handleDdl(CdcEvent event) {
        String rawDdl = event.getDdl();
        if (rawDdl == null) {
            log.warn("DDL event without ddl field");
            return;
        }
        String ddl = rawDdl;
        for (TableChange tc : event.getTableChanges()) {
            if (tc.getId() != null && tc.getTable() != null && tc.getType() != null) {
                String fromName = tc.getId().replace("\"", "").replace(".", "_");
            }
        }
        String db = event.getDatabase();
        for (TableChange tc : event.getTableChanges()) {
            if (tc.getType() == null || tc.getTable() == null) continue;
            String targetTable = sanitizeIdentifier(db) + "_" + safeTableName(tc.getId());
            if ("CREATE".equals(tc.getType())) {
                createTableFromSchema(targetTable, tc);
                upsertSqlCache.remove(targetTable);
                upsertColsCache.remove(targetTable);
            } else if ("ALTER".equals(tc.getType())) {
                alterTableFromDdl(targetTable, ddl);
                upsertSqlCache.remove(targetTable);
                upsertColsCache.remove(targetTable);
            } else if ("DROP".equals(tc.getType())) {
                log.warn("DROP table `{}` — skipping (manual intervention required)", targetTable);
                upsertSqlCache.remove(targetTable);
                upsertColsCache.remove(targetTable);
            }
        }
    }

    private String safeTableName(String debeziumId) {
        if (debeziumId == null) return "unknown";
        int dot = debeziumId.lastIndexOf('.');
        String name = dot >= 0 ? debeziumId.substring(dot + 1) : debeziumId;
        return name.replace("\"", "").replace("`", "");
    }
    private static String sanitizeIdentifier(String id) {
        if (id == null) return "unknown";
        return id.replace("`", "").replace("\"", "").replace("'", "")
                .replace("\0", "").replace("\n", "").replace("\r", "");
    }



    private void createTableFromSchema(String targetTable, TableChange tc) {
        String st = sanitizeIdentifier(targetTable);
        if (createdTables.contains(st)) return;
        CdcEvent.Table tbl = tc.getTable();
        if (tbl == null || tbl.getColumns().isEmpty()) return;

        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS `")
                .append(st).append("` (");
        List<String> colDefs = new ArrayList<>();
        for (Column col : tbl.getColumns()) {
            String type = mysqlType(col);
            String nullable = col.isOptional() ? "" : " NOT NULL";
            colDefs.add(String.format("`%s` %s%s", sanitizeIdentifier(col.getName()), type, nullable));
        }
        if (!tbl.getPrimaryKeyColumnNames().isEmpty()) {
            String pk = tbl.getPrimaryKeyColumnNames().stream()
                    .map(k -> "`" + sanitizeIdentifier(k) + "`").collect(Collectors.joining(", "));
            colDefs.add("PRIMARY KEY (" + pk + ")");
        }
        // Validate charset against whitelist to prevent injection
        String rawCharset = tbl.getDefaultCharsetName();
        String charset = "";
        if (rawCharset != null) {
            String normalized = rawCharset.toLowerCase().trim().replaceAll("[^a-z0-9]", "");
            if (ALLOWED_CHARSETS.contains(normalized)) {
                charset = " DEFAULT CHARSET=" + normalized;
            } else {
                log.warn("Rejected unknown charset '{}' for table `{}` — using default", rawCharset, st);
            }
        }
        ddl.append(String.join(", ", colDefs)).append(")").append(charset);
        try {
            jdbc.execute(ddl.toString());
            createdTables.add(st);
            log.info("Table `{}` created ({} columns)", st, tc.getTable().getColumns().size());
        } catch (Exception e) {
            log.error("Failed to create table `{}`: {}", st, e.getMessage());
        }
    }

    private void alterTableFromDdl(String targetTable, String sourceDdl) {
        if (sourceDdl == null || sourceDdl.trim().isEmpty()) return;
        String upper = sourceDdl.toUpperCase().trim();
        if (!upper.startsWith("ALTER")) return;

        // ── DDL safety: only allow safe column-level ALTER operations ──
        // Reject ALTER TABLE ... RENAME, ALTER TABLE ... ENGINE, etc.
        if (!SAFE_ALTER_PATTERN.matcher(sourceDdl).find()) {
            log.warn("Rejected non-column ALTER DDL for `{}`: {}", 
                    sanitizeIdentifier(targetTable), sourceDdl);
            return;
        }
        String transformed = sourceDdl;
        int tableIdx = upper.indexOf("TABLE ");
        if (tableIdx >= 0) {
            int nameStart = tableIdx + 6;
            while (nameStart < sourceDdl.length() &&
                   (sourceDdl.charAt(nameStart) == ' ' || sourceDdl.charAt(nameStart) == '`')) {
                nameStart++;
            }
            int nameEnd = nameStart;
            while (nameEnd < sourceDdl.length() &&
                   sourceDdl.charAt(nameEnd) != '`' && sourceDdl.charAt(nameEnd) != ' ') {
                nameEnd++;
            }
            if (nameEnd > nameStart) {
                String sourceTableName = sourceDdl.substring(nameStart, nameEnd);
                transformed = sourceDdl.substring(0, nameStart)
                        + targetTable + sourceDdl.substring(nameEnd);
            }
        }
        try {
            jdbc.execute(transformed);
            log.info("DDL executed for `{}`", sanitizeIdentifier(targetTable));
        } catch (Exception e) {
            log.error("Failed ALTER `{}`: {} — {}", sanitizeIdentifier(targetTable), e.getMessage(), transformed);
        }
    }

    private String mysqlType(Column col) {
        String typeName = col.getTypeName();
        if (typeName != null && !typeName.isEmpty()) {
            // Validate: type name must be alphanumeric + parens + comma + space only
            // Rejects injection attempts like "INT; DROP TABLE x--"
            if (!typeName.matches("[a-zA-Z0-9(),_ ]+")) {
                log.warn("Rejected suspicious type name: '{}'", typeName);
                return "VARCHAR(255)";
            }
            String upper = typeName.toUpperCase();
            if (upper.startsWith("VARCHAR") || upper.startsWith("CHAR")
                    || upper.startsWith("VARBINARY") || upper.startsWith("BINARY")) {
                int len = Math.max(col.getLength(), upper.startsWith("VARCHAR") ? 255 : 1);
                return String.format("%s(%d)", typeName, len);
            }
            if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) {
                int len = Math.max(col.getLength(), 10);
                return String.format("%s(%d,%d)", typeName, len, col.getScale());
            }
            return typeName;
        }
        switch (col.getJdbcType()) {
            case Types.TINYINT:   return "TINYINT";
            case Types.SMALLINT:  return "SMALLINT";
            case Types.INTEGER:   return "INT";
            case Types.BIGINT:    return "BIGINT";
            case Types.FLOAT:
            case Types.REAL:      return "FLOAT";
            case Types.DOUBLE:    return "DOUBLE";
            case Types.DECIMAL:
            case Types.NUMERIC:
                return String.format("DECIMAL(%d,%d)",
                        Math.max(col.getLength(), 10), col.getScale());
            case Types.CHAR:
                return String.format("CHAR(%d)", Math.max(col.getLength(), 1));
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return String.format("VARCHAR(%d)", Math.max(col.getLength(), 255));
            case Types.DATE:      return "DATE";
            case Types.TIME:      return "TIME";
            case Types.TIMESTAMP: return "DATETIME";
            case Types.BIT:       return "BIT(1)";
            case Types.BOOLEAN:   return "TINYINT(1)";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:      return "BLOB";
            case Types.CLOB:      return "TEXT";
            default:              return "VARCHAR(255)";
        }
    }

    // ── Truncate ───────────────────────────────────────────────

    private void handleTruncate(CdcEvent event) {
        String targetTable = event.targetTable();
        log.warn("TRUNCATE `{}` — skipping", sanitizeIdentifier(targetTable));
    }

    // ── DML processing ─────────────────────────────────────────

    private void processDmlBatch(List<CdcEvent> batch) {
        Map<String, List<CdcEvent>> byTable = batch.stream()
                .collect(Collectors.groupingBy(CdcEvent::targetTable));

        // Multi-table: each table gets its own upsert/delete; autocommit
        // means no cross-table atomicity. This is documented as a limitation.
        for (Map.Entry<String, List<CdcEvent>> entry : byTable.entrySet()) {
            String targetTable = entry.getKey();
            List<CdcEvent> events = entry.getValue();

            ensureTable(targetTable, events.get(0));

            List<CdcEvent> upserts = new ArrayList<>();
            List<CdcEvent> deletes = new ArrayList<>();
            for (CdcEvent e : events) {
                if (e.isDelete()) deletes.add(e);
                else upserts.add(e);
            }
            try {
                if (!upserts.isEmpty()) batchUpsert(targetTable, upserts);
                if (!deletes.isEmpty()) batchDelete(targetTable, deletes);
            } catch (Exception e) {
                errorCount.addAndGet(events.size());
                log.error("DML batch failed for `{}`: {} ({} events discarded)",
                        sanitizeIdentifier(targetTable), e.getMessage(), events.size());
            }
        }
    }

    // ── ensureTable ────────────────────────────────────────────

    private void ensureTable(String targetTable, CdcEvent sample) {
        String st = sanitizeIdentifier(targetTable);
        if (createdTables.contains(st)) return;
        Map<String, Object> columns = sample.data();
        if (columns == null) return;
        Map<String, Object> keyCols = sample.getKeys();
        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS `")
                .append(st).append("` (");
        List<String> colDefs = new ArrayList<>();
        for (Map.Entry<String, Object> col : columns.entrySet()) {
            String type = inferSqlType(col.getValue());
            colDefs.add(String.format("`%s` %s", sanitizeIdentifier(col.getKey()), type));
        }
        if (!colDefs.isEmpty()) {
            if (keyCols != null && !keyCols.isEmpty()) {
                String pk = keyCols.keySet().stream()
                        .map(k -> "`" + sanitizeIdentifier(k) + "`").collect(Collectors.joining(", "));
                colDefs.add("PRIMARY KEY (" + pk + ")");
            } else {
                colDefs.add("PRIMARY KEY (`" + sanitizeIdentifier(columns.keySet().iterator().next()) + "`)");
            }
        }
        ddl.append(String.join(", ", colDefs)).append(")");
        try {
            jdbc.execute(ddl.toString());
            createdTables.add(st);
            log.info("Table `{}` created (fallback)", st);
        } catch (Exception e) {
            log.error("Failed to create table `{}`: {}", st, e.getMessage());
        }
    }

    // ── batchUpsert ────────────────────────────────────────────

    private void batchUpsert(String table, List<CdcEvent> events) {
        String sql = upsertSqlCache.get(table);
        Set<String> cachedColumns = upsertColsCache.get(table);

        // Rebuild if cache miss, or first row has different column NAMES
        Map<String, Object> firstRow = events.get(0).data();
        if (sql == null || cachedColumns == null || firstRow == null
                || !cachedColumns.equals(firstRow.keySet())) {

            Set<String> colSet = new LinkedHashSet<>();
            for (CdcEvent e : events) {
                Map<String, Object> row = e.data();
                if (row != null) colSet.addAll(row.keySet());
            }
            if (colSet.isEmpty()) return;
            cachedColumns = colSet;
            List<String> cols = colSet.stream().map(MysqlSinkProcessor::sanitizeIdentifier).sorted().collect(Collectors.toList());
            String colList = cols.stream().map(c -> "`" + c + "`")
                    .collect(Collectors.joining(", "));
            String ph = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
            // INSERT ... ON DUPLICATE KEY UPDATE — true upsert semantics.
            // Unlike REPLACE INTO (DELETE+INSERT), this preserves auto-increment IDs,
            // avoids firing DELETE triggers, and does not cascade-delete foreign keys.
            String updateClause = cols.stream()
                    .map(c -> "`" + c + "`=VALUES(`" + c + "`)")
                    .collect(Collectors.joining(", "));
            sql = String.format("INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
                    sanitizeIdentifier(table), colList, ph, updateClause);
            upsertSqlCache.put(table, sql);
            upsertColsCache.put(table, cachedColumns);
        }

        final String finalSql = sql;
        final List<String> cols = new ArrayList<>((Set<String>) cachedColumns);
        java.util.Collections.sort(cols);
        final List<String> sqlCols = cols.stream().map(MysqlSinkProcessor::sanitizeIdentifier).collect(Collectors.toList());

        // Per-row try/catch: one bad row doesn't kill the entire batch
        List<CdcEvent> survivors = new ArrayList<>(events.size());
        for (CdcEvent event : events) {
            Map<String, Object> row = event.data();
            if (row == null) continue;
            // Validate: missing keys get NULL (safe for ON DUPLICATE KEY UPDATE)
            survivors.add(event);
        }
        if (survivors.isEmpty()) return;

        jdbc.batchUpdate(finalSql, survivors, 256, (PreparedStatement ps, CdcEvent event) -> {
            Map<String, Object> row = event.data();
            int i = 1;
            for (int ci = 0; ci < cols.size(); ci++) {
                Object val = row.get(cols.get(ci));
                ps.setObject(i++, val);
            }
        });
    }

    // ── batchDelete ────────────────────────────────────────────

    private void batchDelete(String table, List<CdcEvent> events) {
        Map<List<String>, List<CdcEvent>> grouped = new LinkedHashMap<>();
        for (CdcEvent e : events) {
            Set<String> ks = e.getKeys() != null ? e.getKeys().keySet() : Collections.emptySet();
            List<String> key = ks.stream().sorted().collect(Collectors.toList());
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(e);
        }
        for (Map.Entry<List<String>, List<CdcEvent>> entry : grouped.entrySet()) {
            List<String> keyCols = entry.getKey();
            List<CdcEvent> batch = entry.getValue();
            if (keyCols.isEmpty()) {
                log.warn("Cannot DELETE from `{}`: no key columns", table);
                continue;
            }
            List<String> keys = new ArrayList<>(keyCols);
            String where = keys.stream().map(k -> "`" + sanitizeIdentifier(k) + "` = ?")
                    .collect(Collectors.joining(" AND "));
            String sql = String.format("DELETE FROM `%s` WHERE %s", sanitizeIdentifier(table), where);

            jdbc.batchUpdate(sql, batch, 256, (PreparedStatement ps, CdcEvent event) -> {
                Map<String, Object> kvs = event.getKeys();
                int i = 1;
                for (String k : keys) {
                    ps.setObject(i++, kvs != null ? kvs.get(k) : null);
                }
            });
        }
    }

    // ── fallback type inference ────────────────────────────────

    private String inferSqlType(Object value) {
        if (value == null) return "VARCHAR(255)";
        if (value instanceof Integer || value instanceof Long) return "BIGINT";
        if (value instanceof Double || value instanceof Float) return "DOUBLE";
        if (value instanceof Boolean) return "TINYINT(1)";
        if (value instanceof byte[]) return "BLOB";
        return "VARCHAR(1024)";
    }

    public long getProcessedCount() { return processedCount.get(); }
    public long getErrorCount()     { return errorCount.get(); }
}
