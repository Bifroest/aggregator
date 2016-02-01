package io.bifroest.aggregator.systems.cassandra;

import static org.mockito.Mockito.verify;

import java.time.Duration;

import io.bifroest.retentions.RetentionConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CassandraAccessLayerDryRunTests {
    @Mock
    public CassandraClusterWrapper cluster;

    @Mock
    public RetentionConfiguration retentionConfiguration;

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
}