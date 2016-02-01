package io.bifroest.aggregator.systems.cassandra;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.aggregator.systems.cassandra.statistics.CreateTableEvent;
import io.bifroest.aggregator.systems.cassandra.statistics.DropTableEvent;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionTable;

public class CassandraAccessLayer {

    private static final Logger log = LogManager.getLogger();

    private static final String COL_NAME = "metric";
    private static final String COL_TIME = "timestamp";
    private static final String COL_VALUE = "value";

    private final String user;
    private final String pass;
    private final String keyspace;
    private final String[] hosts;
    private final Duration readTimeout;
    private final Duration waitAfterWriteTimeout;

    private final RetentionConfiguration retention;
    private CassandraClusterWrapper wrappedCluster;
    private Session session = null;

    private final boolean dryRun;

    public CassandraAccessLayer( String user, String pass, String keyspace, String[] hosts, RetentionConfiguration retention, boolean dryRun, Duration readTimeout, Duration waitAfterWriteTimeout ) {
        this.retention = retention;
        this.user = user;
        this.pass = pass;
        this.keyspace = keyspace;
        this.hosts = hosts;
        this.dryRun = dryRun;
        this.readTimeout = readTimeout;
        this.waitAfterWriteTimeout = waitAfterWriteTimeout;

        if ( dryRun ) {
            log.warn( "Running with dryRun, NOT ACTUALLY DOING ANYTHING!!!" );
        }
    }

    public void open() {
        session = wrappedCluster.open();
    }

    public void close() {
        if ( session != null ) {
            session.close();
            session = null;
        }
        if ( wrappedCluster != null ) {
            wrappedCluster.close();
        }
    }

    public Iterable<RetentionTable> loadTables() {
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
        if ( session == null ) {
            open();
        }
        Statement stm = QueryBuilder.select().distinct().column( COL_NAME ).from( table.tableName() );
        final Iterator<Row> iter = session.execute( stm ).iterator();
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
        if ( session == null ) {
            open();
        }
        Clause cName = QueryBuilder.eq( COL_NAME, name );
        Statement stm = QueryBuilder.select().all().from( table.tableName() ).where( cName );
        final Iterator<Row> iter = session.execute( stm ).iterator();
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

        if ( session == null ) {
            open();
        }

        for ( Metric metric : metrics ) {
            String[] columns = { COL_NAME, COL_TIME, COL_VALUE };
            Object[] values = { metric.name(), metric.timestamp(), metric.value() };
            Statement stm = QueryBuilder.insertInto( table.tableName() ).values( columns, values );

            try {
                session.execute( stm );
            } catch ( WriteTimeoutException e ) {
                log.info( "WriteTimeoutException while sending Metrics to cassandra." );
                log.info( e.getMessage() );
                log.info( "According to http://www.datastax.com/dev/blog/how-cassandra-deals-with-replica-failure, this is harmless" );

                try {
                    Thread.sleep( waitAfterWriteTimeout.toMillis() );
                } catch ( InterruptedException e1 ) {
                    // ignore
                }
            }
        }
    }

    public void createTableIfNecessary( RetentionTable table ) {
        if ( session == null ) {
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

        StringBuilder query = new StringBuilder();
        query.append( "CREATE TABLE IF NOT EXISTS " ).append( table.tableName() ).append( " (" );
        query.append( COL_NAME ).append( " text, " );
        query.append( COL_TIME ).append( " bigint, " );
        query.append( COL_VALUE ).append( " double, " );
        query.append( "PRIMARY KEY (" ).append( COL_NAME ).append( ", " ).append( COL_TIME ).append( ")" );
        query.append( ");" );
        session.execute( query.toString() );
        EventBusManager.fire( new CreateTableEvent( System.currentTimeMillis(), table ) );
    }

    public void dropTable( RetentionTable table ) {
        if ( dryRun ) {
            log.debug( "Dropping " + table );
            return;
        }

        if ( session == null ) {
            open();
        }
        session.execute( "DROP TABLE " + table.tableName() + ";" );
        EventBusManager.fire( new DropTableEvent( System.currentTimeMillis(), table ) );
    }

}
