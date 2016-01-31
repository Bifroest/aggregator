package io.bifroest.aggregator.systems.aggregation.statistics;

import java.time.Instant;

import io.bifroest.commons.statistics.EventWithInstant;
import io.bifroest.retentions.RetentionTable;

public class AggregationEvent implements EventWithInstant {
    private final Instant when;
    private final String metricName;
    private final RetentionTable table;
    private final int numValues;

    public AggregationEvent( Instant when, String metricName, RetentionTable table, int numValues ) {
        this.when = when;
        this.metricName = metricName;
        this.table = table;
        this.numValues = numValues;
    }

    public Instant when() {
        return when;
    }

    public String metricName() {
        return metricName;
    }

    public RetentionTable table() {
        return table;
    }

    public int numValues() {
        return numValues;
    }

    @Override
    public String toString() {
        return "AggregationEvent [when=" + when + ", metricName=" + metricName + ", table=" + table + ", numValues=" + numValues + "]";
    }
}
