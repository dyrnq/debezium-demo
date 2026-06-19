package com.dyrnq.debezium.cdc;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.dyrnq.debezium.model.CdcEvent;
import com.dyrnq.debezium.model.CdcEvent.Column;
import com.dyrnq.debezium.model.CdcEvent.Table;
import com.dyrnq.debezium.model.CdcEvent.TableChange;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CdcEventParser {

    public static CdcEvent parse(String keyJson, String valueJson) {
        JSONObject value = JSONUtil.parseObj(valueJson);
        JSONObject payload = value.getJSONObject("payload");
        if (payload == null) return null;

        String op = payload.getStr("op");
        if (op == null) return null;

        Long tsMs = payload.getLong("ts_ms");

        // DDL / Message
        if ("m".equals(op)) {
            return parseDdl(payload, tsMs);
        }

        // Truncate
        if ("t".equals(op)) {
            return parseTruncate(payload, tsMs);
        }

        // DML: c / u / d / r
        return parseDml(payload, keyJson, op, tsMs);
    }

    private static CdcEvent parseDml(JSONObject payload, String keyJson, String op, Long tsMs) {
        JSONObject source = payload.getJSONObject("source");
        if (source == null) return null;

        String db = source.getStr("db");
        String table = source.getStr("table");

        Map<String, Object> after  = mapOrNull(payload.getJSONObject("after"));
        Map<String, Object> before = mapOrNull(payload.getJSONObject("before"));
        Map<String, Object> keys  = parseKeys(keyJson);

        return CdcEvent.builder()
                .op(op).database(db).table(table)
                .after(after).before(before).keys(keys)
                .tsMs(tsMs).build();
    }

    @SuppressWarnings("unchecked")
    private static CdcEvent parseDdl(JSONObject payload, Long tsMs) {
        JSONObject source = payload.getJSONObject("source");
        String db = source != null ? source.getStr("db") : payload.getStr("databaseName");
        String ddl = payload.getStr("ddl");

        List<TableChange> changes = new ArrayList<>();
        JSONArray tcArray = payload.getJSONArray("tableChanges");
        if (tcArray != null) {
            for (Object o : tcArray) {
                JSONObject tc = (JSONObject) o;
                String type = tc.getStr("type");
                String id   = tc.getStr("id");

                JSONObject tbl = tc.getJSONObject("table");
                Table table = null;
                if (tbl != null) {
                    JSONArray cols = tbl.getJSONArray("columns");
                    List<Column> columnList = new ArrayList<>();
                    if (cols != null) {
                        for (Object co : cols) {
                            JSONObject c = (JSONObject) co;
                            columnList.add(Column.builder()
                                    .name(c.getStr("name"))
                                    .jdbcType(c.getInt("jdbcType", 12))
                                    .typeName(c.getStr("typeName"))
                                    .length(c.getInt("length", 0))
                                    .scale(c.getInt("scale", 0))
                                    .optional(c.getBool("optional", true))
                                    .build());
                        }
                    }
                    JSONArray pkCols = tbl.getJSONArray("primaryKeyColumnNames");
                    List<String> pkList = new ArrayList<>();
                    if (pkCols != null) {
                        for (Object pk : pkCols) {
                            pkList.add((String) pk);
                        }
                    }
                    table = Table.builder()
                            .defaultCharsetName(tbl.getStr("defaultCharsetName"))
                            .primaryKeyColumnNames(pkList)
                            .columns(columnList)
                            .build();
                }

                changes.add(TableChange.builder()
                        .type(type).id(id).table(table).build());
            }
        }

        return CdcEvent.builder()
                .op("m").database(db).ddl(ddl)
                .tableChanges(changes).tsMs(tsMs).build();
    }

    private static CdcEvent parseTruncate(JSONObject payload, Long tsMs) {
        JSONObject source = payload.getJSONObject("source");
        String db    = source != null ? source.getStr("db") : null;
        String table = source != null ? source.getStr("table") : null;

        return CdcEvent.builder()
                .op("t").database(db).table(table).tsMs(tsMs).build();
    }

    private static Map<String, Object> parseKeys(String keyJson) {
        if (keyJson == null) return null;
        try {
            JSONObject key = JSONUtil.parseObj(keyJson);
            JSONObject kp = key.getJSONObject("payload");
            return kp != null ? mapOrNull(kp) : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<String, Object> mapOrNull(JSONObject obj) {
        if (obj == null) return null;
        Map<String, Object> map = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : obj.entrySet()) {
            map.put(e.getKey(), e.getValue());
        }
        return map.isEmpty() ? null : map;
    }
}
