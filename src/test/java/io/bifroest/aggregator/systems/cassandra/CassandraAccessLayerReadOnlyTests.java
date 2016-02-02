package io.bifroest.aggregator.systems.cassandra;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.datastax.driver.core.Session;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionTable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAccessLayerReadOnlyTests {
    @Mock
    public CassandraClusterWrapper cluster;

    @Mock
    public RetentionConfiguration retentionConfiguration;

    @Mock
    public Session session;

    public boolean anyDryRunValue = true;

    public Duration waitBetweenWrites = Duration.ZERO;

    public CassandraAccessLayer subject;

    @Before
    public void createSubject() {
        MockitoAnnotations.initMocks(this);
        subject = new CassandraAccessLayer(cluster, retentionConfiguration, anyDryRunValue, waitBetweenWrites);
    }

    @Test
    public void noTablesInCassandraResultInNoRetentionTables() {
        when(cluster.getTableNames()).thenReturn(Collections.emptyList());

        Collection<RetentionTable> retentionTables = subject.loadTables();

        assertThat(retentionTables.isEmpty(), is(true));
    }

    @Test
    public void tablesWithMalformedNamesAreRejected() {
        when(cluster.getTableNames()).thenReturn(Arrays.asList("invalid_name_1", "invalid_name_2"));

        Collection<RetentionTable> retentionTables = subject.loadTables();

        assertThat(retentionTables.isEmpty(), is(true));
    }
}
