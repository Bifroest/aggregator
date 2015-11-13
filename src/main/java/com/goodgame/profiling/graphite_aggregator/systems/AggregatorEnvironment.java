package com.goodgame.profiling.graphite_aggregator.systems;

import java.nio.file.Path;

import com.goodgame.profiling.commons.boot.InitD;
import com.goodgame.profiling.commons.systems.common.AbstractCommonEnvironment;
import com.goodgame.profiling.graphite_aggregator.systems.cassandra.CassandraAccessLayer;
import com.goodgame.profiling.graphite_aggregator.systems.cassandra.EnvironmentWithMutableCassandra;
import com.goodgame.profiling.graphite_retentions.RetentionConfiguration;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithMutableRetentionStrategy;

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
