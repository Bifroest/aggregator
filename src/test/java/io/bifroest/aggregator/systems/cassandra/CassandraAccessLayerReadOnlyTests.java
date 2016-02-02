package io.bifroest.aggregator.systems.cassandra;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import com.datastax.driver.core.Session;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionLevel;
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

    @Test
    public void tablesWithoutARetentionLevelAreRejected() {
        String someLevelName = "level";

        when(cluster.getTableNames()).thenReturn(Arrays.asList("g" + someLevelName + RetentionTable.SEPARATOR_OF_MADNESS + "42"));
        when(retentionConfiguration.getLevelForName(someLevelName)).thenReturn(Optional.empty());

        Collection<RetentionTable> retentionTables = subject.loadTables();

        assertThat(retentionTables.isEmpty(), is(true));
        verify(retentionConfiguration).getLevelForName(someLevelName);
    }

    @Test
    public void tablesWithRetentionLevelAreLoadedAndReturned() {
        String someLevelName = "precise";
        long someBlockIndex = 509;

        // duplication & extremely verbose
        long someSecondsPerDataPoint = 20;
        long someBlockNumber = 10;
        long someBlockSize = 40;
        String noNextLevel = null;

        RetentionLevel retentionLevel = new RetentionLevel(someLevelName, someSecondsPerDataPoint, someBlockNumber, someBlockSize, noNextLevel);

        when(cluster.getTableNames()).thenReturn(Arrays.asList("g" + someLevelName + RetentionTable.SEPARATOR_OF_MADNESS + someBlockIndex));
        when(retentionConfiguration.getLevelForName(someLevelName)).thenReturn(Optional.of(retentionLevel));

        Collection<RetentionTable> retentionTables = subject.loadTables();

        assertThat(retentionTables.size(), is(1));

        RetentionTable createdTable = retentionTables.iterator().next();

        assertThat(createdTable.level(), is(retentionLevel));
        assertThat(createdTable.block(), is(someBlockIndex));
    }
}
