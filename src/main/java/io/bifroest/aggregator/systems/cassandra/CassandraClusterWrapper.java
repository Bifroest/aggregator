package io.bifroest.aggregator.systems.cassandra;

import java.util.Collection;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * This interface exists so we can mock the Cluster-instance in
 * the cassandra access layer, so we can start testing more
 * meaningful stuff here.
 */
public interface CassandraClusterWrapper {
    Session open();
    Collection<String> getTableNames();
    void close();
}
