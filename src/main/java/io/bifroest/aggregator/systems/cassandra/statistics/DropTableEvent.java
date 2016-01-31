package io.bifroest.aggregator.systems.cassandra.statistics;

import io.bifroest.retentions.RetentionTable;

public class DropTableEvent {

    private final long timestamp;
    private final RetentionTable table;

    public DropTableEvent( long timestamp, RetentionTable table ) {
        this.timestamp = timestamp;
        this.table = table;
    }

    public long timestamp() {
        return timestamp;
    }

    public RetentionTable table() {
        return table;
    }

}
