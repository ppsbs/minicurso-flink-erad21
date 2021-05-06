package hpi.des.flink_tutorial.session3.solution;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import org.apache.flink.api.java.functions.KeySelector;

public class Exercise11WhereOperatorSolution implements KeySelector<TaxiRide, Long> {
    @Override
    public Long getKey(TaxiRide value) throws Exception {
        return value.rideId;
    }
}
