package io.bifroest.aggregator.systems.aggregation;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.bifroest.commons.model.Interval;
import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.aggregation.MaxAggregation;
import io.bifroest.commons.statistics.eventbus.EventBusImpl;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.aggregator.systems.AggregatorEnvironment;
import io.bifroest.aggregator.systems.cassandra.CassandraAccessLayer;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionLevel;
import io.bifroest.retentions.RetentionTable;

public class AggregatorTaskTest {

    private final Random random = new Random( 0 );

    private final int NUM_TABLES = 10, NUM_METRICS = 20;

    @Mock
    private RetentionConfiguration retentions;

    @Mock
    private CassandraAccessLayer database;

    @Mock
    private AggregatorEnvironment environment;

    private RetentionLevel nextLevel;
    private List<RetentionTable> tables;
    private List<Metric> metrics;

    @Before
    public void createMocks() {
        EventBusManager.setEventBus( new EventBusImpl() );

        MockitoAnnotations.initMocks( this );
        when( environment.cassandraAccessLayer() ).thenReturn( database );
        when( environment.retentions() ).thenReturn( retentions );
        when( retentions.findFunctionForMetric( anyString() ) ).thenReturn( new MaxAggregation() );

        // Levels and Strategy
        RetentionLevel level = new RetentionLevel( "lvla", 5 * 60, 3, 60 * 60, "lvlb" );
        nextLevel = new RetentionLevel( "lvlb", 60 * 60, 2, 60 * 60 * 24 * 7, null );

        // Names
        String[] names = { "name01", "name02", "name03" };
        when( database.loadMetricNames( any( RetentionTable.class ) ) ).thenReturn( Arrays.asList( names ) );

        // Tables
        tables = new ArrayList<>();
        metrics = new ArrayList<>();
        for ( int i = 0; i < NUM_TABLES; i++ ) {
            RetentionTable table = new RetentionTable( level, random.nextInt( 1000 ) );
            Interval interval = table.getInterval();
            tables.add( table );

            // Metrics
            @SuppressWarnings( "unchecked" )
            List<Metric>[] metricsArray = new List[names.length];
            Arrays.fill( metricsArray, new ArrayList<>() );
            for ( int k = 0; k < NUM_METRICS; k++ ) {
                int nameIdx = random.nextInt( names.length );
                long timestamp = random.nextInt( (int)( interval.end() - interval.start() ) + 1 ) + interval.start();
                table.getInterval();
                Metric metric = new Metric( names[nameIdx], timestamp, random.nextDouble() );
                metricsArray[nameIdx].add( metric );
                metrics.add( metric );
            }
            for ( int k = 0; k < metricsArray.length; k++ ) {
                when( database.loadUnorderedMetrics( table, names[k] ) ).thenReturn( metricsArray[k] );
            }
        }
        when( database.loadTables() ).thenReturn( tables );
    }

    @Test
    public void testAggregatorTask() {
        for ( RetentionTable table : tables ) {
            new AggregatorTask<AggregatorEnvironment>( environment, table, Optional.of( nextLevel ), Duration.ZERO ).run();
        }

        for ( Metric metric : metrics ) {
            TableMatcher tableMatcher = new TableMatcher( nextLevel, metric );
            MetricMatcher metricMatcher = new MetricMatcher( nextLevel, metric );

            verify( database, atLeastOnce() ).createTableIfNecessary( argThat( tableMatcher ) );
            verify( database, atLeastOnce() ).insertMetrics( argThat( tableMatcher ), argThat( metricMatcher ) );
        }
    }

    private class TableMatcher extends ArgumentMatcher<RetentionTable> {

        private final RetentionLevel level;
        private final Metric metric;

        public TableMatcher( RetentionLevel level, Metric metric ) {
            this.level = level;
            this.metric = metric;
        }

        @Override
        public boolean matches( Object argument ) {
            if ( argument instanceof RetentionTable ) {
                RetentionTable table = (RetentionTable)argument;
                return table.level().equals( level ) && table.contains( metric.timestamp() );
            } else {
                return false;
            }
        }

        @Override
        public void describeTo( Description desc ) {
            DateFormat format = new SimpleDateFormat( "yyyy-MM-dd | HH:mm:ss" );
            desc.appendText( String.format( "RetentionTable[ level == %s, must contain timestamp %s ]", level, format.format( metric.timestamp() ) ) );
        }
    }

    private class MetricMatcher extends ArgumentMatcher<Collection<Metric>> {

        private final long slot;

        public MetricMatcher( RetentionLevel level, Metric metric ) {
            this.slot = metric.timestamp() - ( metric.timestamp() % level.frequency() );
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public boolean matches( Object argument ) {
            if ( argument instanceof Collection ) {
                for ( Metric metric : (Collection<Metric>)argument ) {
                    if ( metric.timestamp() != slot ) {
                        return false;
                    }
                }
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void describeTo( Description desc ) {
            DateFormat format = new SimpleDateFormat( "yyyy-MM-dd | HH:mm:ss" );
            desc.appendText( String.format( "Metric[ timestamp == %s]", format.format( this.slot ) ) );
        }
    }
}
