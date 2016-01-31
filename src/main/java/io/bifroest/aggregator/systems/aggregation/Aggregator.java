package io.bifroest.aggregator.systems.aggregation;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.aggregator.systems.aggregation.statistics.AggregationFinishedEvent;
import io.bifroest.aggregator.systems.aggregation.statistics.AggregationStartedEvent;
import io.bifroest.aggregator.systems.aggregation.statistics.SingleAggregationSubmitted;
import io.bifroest.aggregator.systems.aggregation.statistics.SingleAggregationTerminated;
import io.bifroest.aggregator.systems.cassandra.EnvironmentWithCassandra;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.retentions.RetentionLevel;
import io.bifroest.retentions.RetentionTable;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

public class Aggregator<E extends EnvironmentWithCassandra & EnvironmentWithRetentionStrategy & EnvironmentWithJSONConfiguration> implements Runnable {
    private static final Logger log = LogManager.getLogger();

    private final E environment;
    private final ExecutorService executor;
    private final Duration sleepAfterEachTable;

    private volatile boolean running;

    public Aggregator( E environment, int poolsize, Duration sleepAfterEachTable ) {
        this.environment = environment;
        this.executor = Executors.newFixedThreadPool( poolsize );
        this.sleepAfterEachTable = sleepAfterEachTable;
    }

    public void shutdown() throws InterruptedException {
        running = false;
        executor.shutdown();
        executor.awaitTermination( Long.MAX_VALUE, TimeUnit.SECONDS );
    }

    @Override
    public void run() {
        running = true;
        try {
            EventBusManager.synchronousFire( new AggregationStartedEvent( System.currentTimeMillis() ) );

            environment.getConfigurationLoader().loadConfiguration();
            long now = System.currentTimeMillis() / 1000;

            for ( RetentionLevel level : environment.retentions().getTopologicalSort() ) {
                handleLevel( level, environment.retentions().getNextLevel( level ), now );

                // Check for shutdown
                if ( !running ) {
                    EventBusManager.synchronousFire( new AggregationFinishedEvent( System.currentTimeMillis(), false ) );
                    return;
                }
            }

            EventBusManager.synchronousFire( new AggregationFinishedEvent( System.currentTimeMillis(), true ) );
        } catch( Exception e ) {
            log.warn( "A totally unexpected exception occured", e );
            EventBusManager.synchronousFire( new AggregationFinishedEvent( System.currentTimeMillis(), false ) );
        }
    }

    private void handleLevel( RetentionLevel current, Optional<RetentionLevel> next, long now ) {
        log.entry( current, next, now );

        Collection<Future<?>> futures = new ArrayList<>();

        for ( RetentionTable table : environment.cassandraAccessLayer().loadTables() ) {

            // Skip any uninteresting tables
            if ( !current.equals( table.level() ) ) {
                log.trace( "Skipping {} due to level.", table );
                continue;

            } else if ( table.block() > current.indexOf( now ) - current.blocks() ) {
                log.trace( "Skipping {} due to time. table.block={}, current.indexOf(now)={}, current.blocks()={}",
                        table, table.block(), current.indexOf( now ), current.blocks() );
                continue;

            } else {
                log.trace( "Submitting " + table );
                futures.add( executor.submit( new AggregatorTask<E>( environment, table, next, sleepAfterEachTable ) ) );
                EventBusManager.fire( new SingleAggregationSubmitted( Clock.systemUTC() ) );
            }
        }

        // Wait until all threads are done
        for ( Future<?> future : futures ) {
            try {
                future.get();
                EventBusManager.fire( new SingleAggregationTerminated( Clock.systemUTC(), true ) );
            } catch( InterruptedException | ExecutionException e ) {
                EventBusManager.fire( new SingleAggregationTerminated( Clock.systemUTC(), false ) );
                log.warn( "Aggregation Interrupted", e );
            }
        }

        log.exit();
    }

}
