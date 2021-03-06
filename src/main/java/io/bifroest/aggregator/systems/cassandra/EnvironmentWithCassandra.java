package io.bifroest.aggregator.systems.cassandra;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithCassandra extends Environment {

    CassandraAccessLayer cassandraAccessLayer();

}
