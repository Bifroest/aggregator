package com.goodgame.profiling.graphite_aggregator.systems.cassandra;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.boot.interfaces.Subsystem;
import com.goodgame.profiling.commons.statistics.units.parse.DurationParser;
import com.goodgame.profiling.commons.systems.SystemIdentifiers;
import com.goodgame.profiling.commons.systems.configuration.EnvironmentWithJSONConfiguration;
import com.goodgame.profiling.commons.util.json.JSONUtils;
import com.goodgame.profiling.graphite_aggregator.systems.AggregatorIdentifiers;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;

@MetaInfServices
public class CassandraSystem<E extends EnvironmentWithJSONConfiguration & EnvironmentWithRetentionStrategy & EnvironmentWithMutableCassandra> implements
        Subsystem<E> {

    private CassandraAccessLayer cassandra;

    @Override
    public String getSystemIdentifier() {
        return AggregatorIdentifiers.CASSANDRA;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.RETENTION, SystemIdentifiers.STATISTICS );
    }

    @Override
    public void configure( JSONObject configuration ) {
        // empty
    }

    @Override
    public void boot( E environment ) throws Exception {
        JSONObject config = environment.getConfiguration().getJSONObject( "cassandra" );
        String username = config.optString( "username", null );
        String password = config.optString( "password", null );
        String keyspace = config.getString( "keyspace" );
        String[] seeds = JSONUtils.getStringArray( "seeds", config );
        boolean dryRun = config.optBoolean( "dry-run", false );
        Duration readTimeout = config.has( "read-timeout" ) ? new DurationParser().parse( config.getString( "read-timeout" ) ) : Duration.ofSeconds( 12 );
        Duration waitAfterWriteTimeout = config.has( "wait-after-write-timeout" ) ? new DurationParser().parse( config.getString( "wait-after-write-timeout" ) ) : Duration.ZERO;
        cassandra = new CassandraAccessLayer( username, password, keyspace, seeds, environment.retentions(), dryRun, readTimeout, waitAfterWriteTimeout );

        cassandra.open();
        environment.setCassandraAccessLayer( cassandra );
    }

    @Override
    public void shutdown( E environment ) {
        cassandra.close();
    }
}
