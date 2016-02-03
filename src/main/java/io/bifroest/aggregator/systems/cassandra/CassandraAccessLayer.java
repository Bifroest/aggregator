package io.bifroest.aggregator.systems.cassandra;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Row;
import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.aggregator.systems.cassandra.statistics.CreateTableEvent;
import io.bifroest.aggregator.systems.cassandra.statistics.DropTableEvent;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionTable;

public class CassandraAccessLayer {

    private static final Logger log = LogManager.getLogger();

    static final String COL_NAME = "metric";
    static final String COL_TIME = "timestamp";
    static final String COL_VALUE = "value";

    private final RetentionConfiguration retention;
    private final CassandraClusterWrapper wrappedCluster;
    private CassandraSession cassandraSession;

    private final boolean dryRun;

    public CassandraAccessLayer( CassandraClusterWrapper wrappedCluster, RetentionConfiguration retention, boolean dryRun)  {
        this.retention = retention;
        this.dryRun = dryRun;
        this.wrappedCluster = wrappedCluster;

        if ( dryRun ) {
            log.warn( "Running with dryRun, NOT ACTUALLY DOING ANYTHING!!!" );
        }
    }

    public void open() {
        cassandraSession = wrappedCluster.open();
    }

    public void close() {
        if (cassandraSession != null ) {
            cassandraSession.close();
            cassandraSession = null;
        }
        if ( wrappedCluster != null ) {
            wrappedCluster.close();
        }
    }

    public Collection<RetentionTable> loadTables() {
        List<RetentionTable> ret = new ArrayList<>();

        Collection<String> tableNames = wrappedCluster.getTableNames();

        for ( String tableName : tableNames ) {
            if ( RetentionTable.canCreateTable( tableName, retention ) ) {
                ret.add( new RetentionTable( tableName, retention ) );
            } else {
                log.warn( "Table " + tableName + " doesn't match format." );
            }
        }

        return ret;
    }

    public Iterable<String> loadMetricNames( RetentionTable table ) {
        if (cassandraSession == null ) {
            open();
        }
        final Iterator<Row> iter = cassandraSession.loadNamesFromTable(table);
        return new Iterable<String>() {

            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public String next() {
                        Row row = iter.next();
                        return row.getString( COL_NAME );
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Iterable<Metric> loadUnorderedMetrics( RetentionTable table, String name ) {
        if (cassandraSession == null ) {
            open();
        }
        final Iterator<Row> iter = cassandraSession.loadMetricsFromTable(table, name);
        return new Iterable<Metric>() {

            @Override
            public Iterator<Metric> iterator() {
                return new Iterator<Metric>() {

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public Metric next() {
                        Row row = iter.next();
                        return new Metric( row.getString( COL_NAME ), row.getLong( COL_TIME ), row.getDouble( COL_VALUE ) );
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }


    public void insertMetrics( RetentionTable table, Collection<Metric> metrics ) {
        if ( dryRun ) {
            log.debug( "Inserting " + metrics.toString() + " into " + table );
            return;
        }

        if (cassandraSession == null ) {
            open();
        }

        for ( Metric metric : metrics ) {
            cassandraSession.insertMetric(table, metric);
        }
    }

    public void createTableIfNecessary( RetentionTable table ) {
        if (cassandraSession == null ) {
            open();
        }
        Collection<String> tableNames = wrappedCluster.getTableNames();
        for ( String tableName : tableNames ) {
            if ( tableName.equalsIgnoreCase( table.tableName() ) ) {
                return;
            }
        }

        if ( dryRun ) {
            log.debug( "Creating table " + table );
            return;
        }

        cassandraSession.createTable(table);
        EventBusManager.fire( new CreateTableEvent( System.currentTimeMillis(), table ) );
    }

    public void dropTable( RetentionTable table ) {
        if ( dryRun ) {
            log.debug( "Dropping " + table );
            return;
        }

        if (cassandraSession == null ) {
            open();
        }
        cassandraSession.dropTableInDatabase(table);
        EventBusManager.fire( new DropTableEvent( System.currentTimeMillis(), table ) );
    }
}
