package io.bifroest.aggregator.systems.aggregation;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.exceptions.DriverException;
import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.aggregator.systems.aggregation.statistics.AggregationEvent;
import io.bifroest.aggregator.systems.cassandra.CassandraAccessLayer;
import io.bifroest.aggregator.systems.cassandra.EnvironmentWithCassandra;
import io.bifroest.retentions.RetentionLevel;
import io.bifroest.retentions.RetentionTable;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

public class AggregatorTask<E extends EnvironmentWithCassandra & EnvironmentWithRetentionStrategy> implements Runnable {
    private static final Logger log = LogManager.getLogger();
    private static final Clock clock = Clock.systemUTC();

    private final E environment;
    private final RetentionTable table;
    private final Optional<RetentionLevel> nextLevel;
    private final Duration sleepAfter;

    public AggregatorTask( E environment, RetentionTable table, Optional<RetentionLevel> nextLevel, Duration sleepAfter ) {
        this.environment = Objects.requireNonNull( environment );
        this.table = Objects.requireNonNull( table );
        this.nextLevel = nextLevel;
        this.sleepAfter = sleepAfter;
    }

    @Override
    public void run() {
        log.info( "Aggregating from table {} to level {}", table, nextLevel );

        try {
            if ( nextLevel.isPresent() ) {
                for ( String name : environment.cassandraAccessLayer().loadMetricNames( table ) ) {
                    handleMetrics( table, nextLevel.get(), name );
                }
            }
            environment.cassandraAccessLayer().dropTable( table );

            Thread.sleep( sleepAfter.toMillis() );

        } catch( DriverException e ) {
            log.warn( "A problem with Cassandra occured", e );
        } catch( Exception e ) {
            log.warn( "A totally unexpected exception occured", e );
        }
    }

    private void handleMetrics( RetentionTable source, RetentionLevel targetLevel, String name ) {
        if ( targetLevel.blockSize() % targetLevel.frequency() != 0 ) {
            throw new IllegalStateException( String.format(
                    "targetLevel.frequency(%d) does not divide targetLevel.blockSize(%d)",
                    targetLevel.blockSize(),
                    targetLevel.frequency() ) );
        }
        if ( targetLevel.frequency() % table.level().frequency() != 0 ) {
            throw new IllegalStateException( String.format(
                    "targetLevel.frequency(%d) does not divide table.level().frequency(%d)",
                    targetLevel.frequency(),
                    table.level().frequency() ) );
        }

        RetentionTable target = new RetentionTable( targetLevel, table.getInterval().start() / targetLevel.blockSize() );
        CassandraAccessLayer database = environment.cassandraAccessLayer();
        Collection<Metric> aggregatedMetrics = io.bifroest.retentions.Aggregator.aggregate(
                name,
                database.loadUnorderedMetrics( source, name ),
                source.getInterval(),
                targetLevel.frequency(),
                environment.retentions()
                );

        database.createTableIfNecessary( target );
        database.insertMetrics( target, aggregatedMetrics );
        EventBusManager.fire( new AggregationEvent( clock.instant(), name, target, aggregatedMetrics.size() ) );
    }
}
