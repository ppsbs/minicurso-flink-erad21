package hpi.des.flink_tutorial.session3.solution;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class Exercise11WindowJoinProcessingOperatorSolution implements JoinFunction<TaxiRide, TaxiFare, Tuple5<Long, Short, String, Float, Float>> {
    @Override
    public Tuple5<Long, Short, String, Float, Float> join(TaxiRide first, TaxiFare second) throws Exception {
        Tuple5<Long, Short, String, Float, Float> result = new Tuple5<>();
        result.f0 = first.rideId;
        result.f1 = first.passengerCnt;
        result.f2 = second.paymentType;
        result.f3 = second.totalFare;
        result.f4 = second.tip;
        return result;
    }
}
