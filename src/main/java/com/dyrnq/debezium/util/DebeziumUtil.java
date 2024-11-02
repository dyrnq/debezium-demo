package com.dyrnq.debezium.util;

import cn.hutool.json.JSONUtil;
import com.github.f4b6a3.tsid.TsidCreator;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.engine.ChangeEvent;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DebeziumUtil {
    public static void jsonOffsetDataFile(byte[] offsetFileBytes) throws IOException {
        Map<byte[], byte[]> raw = SerializationUtils.deserialize(offsetFileBytes);

        Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
        Map<String, String> dataStr = new HashMap<>();
        for (Map.Entry<byte[], byte[]> entry : raw.entrySet()) {
            ByteBuffer key = entry.getKey() != null ? ByteBuffer.wrap(entry.getKey()) : null;
            ByteBuffer value = entry.getValue() != null ? ByteBuffer.wrap(entry.getValue()) : null;
            data.put(key, value);
            dataStr.put(new String(key.array(), StandardCharsets.UTF_8), new String(value.array(), StandardCharsets.UTF_8));
        }
        System.out.println(JSONUtil.toJsonStr(dataStr));
    }

    public static void jsonOffsetDataFile(File offsetFile) throws IOException {
        jsonOffsetDataFile(FileUtils.readFileToByteArray(offsetFile));
    }

    public static void save(ChangeEvent<String, String> record) {
        save(record, null);
    }

    public static void save(ChangeEvent<String, String> record, File file) {
        if (record.value() == null) return;

        String prettyJson = JSONUtil.toJsonStr(JSONUtil.parse(record.value()), 2);
        OutputStream fileOutputstream = null;
        try {

            if (file == null) {
                long number = TsidCreator.getTsid().toLong();
                file = new File(StringUtils.joinWith(File.separator, "json", String.format("dbz-%s.json", number)));
            }
            FileUtils.createParentDirectories(file);

            fileOutputstream = new FileOutputStream(file);
            IOUtils.write(prettyJson, fileOutputstream, Charset.defaultCharset());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(fileOutputstream);
        }
    }


    public static void defaultMySqlConnectorConfig() {
        java.util.Set<String> set = MySqlConnectorConfig.ALL_FIELDS.allFieldNames();


        Set<String> sortedSet = set.stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String s : sortedSet) {
            String value = MySqlConnectorConfig.ALL_FIELDS.fieldWithName(s).defaultValueAsString();
            String description = MySqlConnectorConfig.ALL_FIELDS.fieldWithName(s).description();
            System.out.printf("%s=%s # %s%n", s, value, description);

        }
    }
}

