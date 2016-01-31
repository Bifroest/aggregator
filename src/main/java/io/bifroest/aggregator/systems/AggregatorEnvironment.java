package io.bifroest.aggregator.systems;

import java.nio.file.Path;

import io.bifroest.commons.boot.InitD;
import io.bifroest.aggregator.systems.cassandra.CassandraAccessLayer;
import io.bifroest.aggregator.systems.cassandra.EnvironmentWithMutableCassandra;
import io.bifroest.commons.environment.AbstractCommonEnvironment;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.bootloader.EnvironmentWithMutableRetentionStrategy;

public class AggregatorEnvironment extends AbstractCommonEnvironment implements EnvironmentWithMutableRetentionStrategy,
        EnvironmentWithMutableCassandra {
    private RetentionConfiguration retention;
    private CassandraAccessLayer cassandra;

    public AggregatorEnvironment( Path configPath, InitD init ) {
        super( configPath, init );
    }

    @Override
    public RetentionConfiguration retentions() {
        return retention;
    }

    @Override
    public void setRetentions( RetentionConfiguration retention ) {
        this.retention = retention;
    }

    @Override
    public CassandraAccessLayer cassandraAccessLayer() {
        return cassandra;
    }

    @Override
    public void setCassandraAccessLayer( CassandraAccessLayer cassandra ) {
        this.cassandra = cassandra;
    }

}
