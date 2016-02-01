package io.bifroest.aggregator.systems.cassandra;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;

/**
 * Productive implementation of the CassandraClusterWrapper.
 *
 * This class should only forward calls to the cluster one-to-one.
 * There should not be logic here, logic should go into classes we can test.
 */
public final class DirectClusterWrapper implements CassandraClusterWrapper {
    private final String keyspace;
    private final String user;
    private final String pass;
    private final String[] hosts;
    private final Duration readTimeout;

    private Cluster cluster;
    private Session session;

    public DirectClusterWrapper(String[] hosts, String keyspace, String user, String pass, Duration readTimeout) {
        this.keyspace = keyspace;
        this.user = user;
        this.pass = pass;
        this.hosts = hosts;
        this.readTimeout = readTimeout;
    }

    @Override
    public Session open() {
        if ( cluster == null || session == null ) {
            Builder builder = Cluster.builder();
            builder.addContactPoints( hosts );
            builder.withSocketOptions( ( new SocketOptions().setReadTimeoutMillis( (int)readTimeout.toMillis() ) ) );
            if ( user != null && pass != null && !user.isEmpty() && !pass.isEmpty() ) {
                builder = builder.withCredentials( user, pass );
            }
            cluster = builder.build();
            session = cluster.connect( keyspace );
        }
        return session;
    }

    @Override
    public Collection<String> getTableNames() {
       return cluster.getMetadata().getKeyspace( keyspace ).getTables().stream().map(TableMetadata::getName).collect(Collectors.toList());
    }

    @Override
    public void close() {
        cluster.close();
    }
}
