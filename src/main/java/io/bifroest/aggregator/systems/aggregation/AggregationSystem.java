package io.bifroest.aggregator.systems.aggregation;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.statistics.units.parse.DurationParser;
import io.bifroest.aggregator.systems.AggregatorIdentifiers;
import io.bifroest.aggregator.systems.cassandra.EnvironmentWithCassandra;
import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.commons.cron.TaskRunner;
import io.bifroest.commons.cron.TaskRunner.TaskID;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

@MetaInfServices
public class AggregationSystem<E extends EnvironmentWithJSONConfiguration & EnvironmentWithRetentionStrategy & EnvironmentWithCassandra>
        implements Subsystem<E> {
    private static final int DEFAULT_POOLSIZE = 10;

    private static final Logger log = LogManager.getLogger();
    private static final DurationParser parser = new DurationParser();

    private Aggregator<E> aggregator;
    private TaskID task;

    @Override
    public String getSystemIdentifier() {
        return AggregatorIdentifiers.AGGREGATION;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.STATISTICS, SystemIdentifiers.RETENTION, AggregatorIdentifiers.CASSANDRA );
    }

    @Override
    public void configure( JSONObject configuration ) {
        // empty
    }

    @Override
    public void boot( final E environment ) {
        JSONObject config = environment.getConfiguration().getJSONObject( "aggregator" );
        int poolsize = config.optInt( "poolsize", DEFAULT_POOLSIZE );
        Duration frequency = parser.parse( config.getString( "frequency" ) );
        Duration sleepAfterEachTable = parser.parse( config.getString( "sleep-after-each-table" ) );

        aggregator = new Aggregator<E>( environment, poolsize, sleepAfterEachTable );
        task = TaskRunner.runRepeated( aggregator, "Aggregator", Duration.ZERO, frequency, false );
    }

    @Override
    public void shutdown( E environment ) {
        try {
            TaskRunner.stopTask( task );
            aggregator.shutdown();
        } catch( InterruptedException e ) {
            log.warn( "Shutdown Interrupted", e );
        }
    }
}
