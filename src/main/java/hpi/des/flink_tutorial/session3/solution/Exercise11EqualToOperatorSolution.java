package hpi.des.flink_tutorial.session3.solution;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import org.apache.flink.api.java.functions.KeySelector;

public class Exercise11EqualToOperatorSolution implements KeySelector<TaxiFare, Long> {
    @Override
    public Long getKey(TaxiFare value) throws Exception {
        return value.rideId;
    }
}
