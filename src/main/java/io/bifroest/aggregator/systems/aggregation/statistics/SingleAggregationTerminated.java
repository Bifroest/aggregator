package io.bifroest.aggregator.systems.aggregation.statistics;

import java.time.Clock;

import io.bifroest.commons.statistics.process.ProcessFinishedEvent;

public class SingleAggregationTerminated extends ProcessFinishedEvent {
    public SingleAggregationTerminated( Clock clock, boolean success ) {
        super( clock, success );
    }
}
