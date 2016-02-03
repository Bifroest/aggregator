package io.bifroest.aggregator.systems.cassandra;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.statistics.units.parse.DurationParser;
import io.bifroest.commons.util.json.JSONUtils;
import io.bifroest.aggregator.systems.AggregatorIdentifiers;
import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

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
        CassandraClusterWrapper wrappedCluster = new DirectClusterWrapper(seeds, keyspace, username, password, readTimeout, waitAfterWriteTimeout);
        cassandra = new CassandraAccessLayer( wrappedCluster, environment.retentions(), dryRun);

        cassandra.open();
        environment.setCassandraAccessLayer( cassandra );
    }

    @Override
    public void shutdown( E environment ) {
        cassandra.close();
    }
}
