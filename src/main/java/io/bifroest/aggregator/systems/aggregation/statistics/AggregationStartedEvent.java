package io.bifroest.aggregator.systems.aggregation.statistics;

import io.bifroest.commons.statistics.process.ProcessStartedEvent;

public class AggregationStartedEvent extends ProcessStartedEvent {

    public AggregationStartedEvent( long timestamp ) {
        super( timestamp );
    }

}
