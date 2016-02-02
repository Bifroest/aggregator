package io.bifroest.aggregator.systems.cassandra;

import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import com.datastax.driver.core.Session;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionLevel;
import io.bifroest.retentions.RetentionTable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAccessLayerDryRunTests {
    @Mock
    public CassandraClusterWrapper cluster;

    @Mock
    public RetentionConfiguration retentionConfiguration;

    @Mock
    public Session session;

    public boolean doDryRun = true;

    public Duration waitBetweenWrites = Duration.ZERO;

    public CassandraAccessLayer subject;

    @Before
    public void createSubject() {
        MockitoAnnotations.initMocks(this);
        subject = new CassandraAccessLayer(cluster, retentionConfiguration, doDryRun, waitBetweenWrites);
    }

    @Test
    public void openDelegatesToCluster() {
        subject.open();

        verify(cluster).open();
    }

    @Test
    public void closeClosesSessionAndCluster() {
        when(cluster.open()).thenReturn(session);

        subject.open();
        subject.close();

        verify(cluster).close();
        verify(session).close();
    }

    @Test
    public void tablesAreNotDroppedDuringDryRun() {
        // duplication & extremely verbose
        String someLevelName = "precise";
        long someSecondsPerDataPoint = 20;
        long someBlockNumber = 10;
        long someBlockSize = 40;
        String noNextLevel = null;
        long someBlockIndex = 42;

        RetentionLevel retentionLevel = new RetentionLevel(someLevelName, someSecondsPerDataPoint, someBlockNumber, someBlockSize, noNextLevel);

        RetentionTable someTable = new RetentionTable(retentionLevel, someBlockIndex);

        subject.dropTable(someTable);

        verifyZeroInteractions(cluster);
    }

    @Test
    public void metricsAreNotInsertedInDryRun() {
        // duplication & extremely verbose
        String someLevelName = "precise";
        long someSecondsPerDataPoint = 20;
        long someBlockNumber = 10;
        long someBlockSize = 40;
        String noNextLevel = null;
        long someBlockIndex = 42;

        RetentionLevel retentionLevel = new RetentionLevel(someLevelName, someSecondsPerDataPoint, someBlockNumber, someBlockSize, noNextLevel);

        RetentionTable someTable = new RetentionTable(retentionLevel, someBlockIndex);

        subject.insertMetrics(someTable, Collections.emptyList());

        verifyZeroInteractions(cluster);
    }

    @Test
    public void createTableIfNecessaryDoesNotCreateTablesInDryRun() {
        // duplication & extremely verbose
        String someLevelName = "precise";
        long someSecondsPerDataPoint = 20;
        long someBlockNumber = 10;
        long someBlockSize = 40;
        String noNextLevel = null;
        long someBlockIndex = 42;

        RetentionLevel retentionLevel = new RetentionLevel(someLevelName, someSecondsPerDataPoint, someBlockNumber, someBlockSize, noNextLevel);

        RetentionTable someTable = new RetentionTable(retentionLevel, someBlockIndex);

        when(cluster.getTableNames()).thenReturn(Collections.emptyList());
        when(cluster.open()).thenReturn(session);

        subject.createTableIfNecessary(someTable);

        verifyZeroInteractions(session);
    }
}
