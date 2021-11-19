package hpi.des.flink_tutorial.session1;

import hpi.des.flink_tutorial.util.TaxiRideTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;

import java.time.LocalDateTime;

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise1Operator implements MapFunction<TaxiRideTuple, Tuple7<LocalDateTime, LocalDateTime,
        Integer, Integer, Integer, Double, Double>> {

    @Override
    public Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double> map(TaxiRideTuple taxiRideTuple) throws Exception {
        return new Tuple7<>(
                taxiRideTuple.f1, taxiRideTuple.f2, taxiRideTuple.f3, taxiRideTuple.f5, taxiRideTuple.f9,
                taxiRideTuple.f13, taxiRideTuple.f16
        );
    }

}

