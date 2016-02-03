package io.bifroest.aggregator.systems.cassandra;

import java.util.Iterator;

import com.datastax.driver.core.Row;
import io.bifroest.commons.model.Metric;
import io.bifroest.retentions.RetentionTable;

public interface CassandraSession {
    void dropTableInDatabase(RetentionTable table);

    void createTable(RetentionTable table);

    Iterator<Row> loadNamesFromTable(RetentionTable table);

    Iterator<Row> loadMetricsFromTable(RetentionTable table, String name);

    void insertMetric(RetentionTable table, Metric metric);

    void close();
}
