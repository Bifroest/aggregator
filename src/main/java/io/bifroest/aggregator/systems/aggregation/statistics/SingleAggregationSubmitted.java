package io.bifroest.aggregator.systems.aggregation.statistics;

import java.time.Clock;

import io.bifroest.commons.statistics.process.ProcessStartedEvent;

public class SingleAggregationSubmitted extends ProcessStartedEvent {
    public SingleAggregationSubmitted( Clock clock ) {
        super( clock );
    }
}
