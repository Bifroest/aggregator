package io.bifroest.aggregator.systems.aggregation.statistics;

import io.bifroest.commons.statistics.process.ProcessFinishedEvent;

public class AggregationFinishedEvent extends ProcessFinishedEvent {

    public AggregationFinishedEvent( long timestamp, boolean success ) {
        super( timestamp, success );
    }

}
