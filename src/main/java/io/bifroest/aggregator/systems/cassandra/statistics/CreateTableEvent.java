package io.bifroest.aggregator.systems.cassandra.statistics;

import io.bifroest.retentions.RetentionTable;

// May be fired multiple times, because createTableIfNecessary doesn't know if if actually created a table.
public class CreateTableEvent {

    private final long timestamp;
    private final RetentionTable table;

    public CreateTableEvent( long timestamp, RetentionTable table ) {
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
