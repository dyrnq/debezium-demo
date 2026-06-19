package com.dyrnq.debezium.cdc;

import com.dyrnq.debezium.model.CdcEvent;

/**
 * Direct-write interface for snapshot events that bypass the ring buffer.
 */
public interface MysqlSinkWriter {
    void write(CdcEvent event);
}
