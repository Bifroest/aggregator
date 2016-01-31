package io.bifroest.aggregator.systems.cassandra.statistics;

import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.eventbus.EventBusRegistrationPoint;
import io.bifroest.commons.statistics.gathering.StatisticGatherer;
import io.bifroest.commons.statistics.storage.MetricStorage;
import io.bifroest.retentions.RetentionTable;

@MetaInfServices
public class CassandraStatusReporter implements StatisticGatherer {

    private static final Logger log = LogManager.getLogger();

    private Set<RetentionTable> tablesCreated = new HashSet<>();

    private volatile long dropped = 0;
    private volatile long created = 0;

    @Override
    public void init() {
        EventBusRegistrationPoint registrationPoint = EventBusManager.createRegistrationPoint();

        registrationPoint.subscribe( CreateTableEvent.class, event -> {
            if ( tablesCreated.contains( event.table() ) ) {
                log.debug( "Got duplicate CreatedTableEvent for {}", event.table().toString() );
            } else {
                created += 1;
                log.info( "===============================================================" );
                log.info( "Created table:" + event.table().toString() );
                log.info( "===============================================================" );
            }
        } );

        registrationPoint.subscribe( DropTableEvent.class, event -> {
            dropped += 1;
            log.info( "===============================================================" );
            log.info( "Dropped table:" + event.table().toString() );
            log.info( "===============================================================" );
        } );

        registrationPoint.subscribe( WriteToStorageEvent.class, e -> {
            MetricStorage storage = e.storageToWriteTo();

            storage.store( "createdTables", created );
            storage.store( "droppedTables", dropped );
        } );
    }
}
