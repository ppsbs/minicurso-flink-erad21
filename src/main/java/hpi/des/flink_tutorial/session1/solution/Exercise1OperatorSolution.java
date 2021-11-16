package hpi.des.flink_tutorial.session1.solution;

import hpi.des.flink_tutorial.util.TaxiRideTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;

import java.time.LocalDateTime;

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise1OperatorSolution implements MapFunction<TaxiRideTuple, Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double>> {

    @Override
    public Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double> map(TaxiRideTuple rideTuple) throws Exception {
        return new Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double>(rideTuple.f1, rideTuple.f2, rideTuple.f3, rideTuple.f5, rideTuple.f9, rideTuple.f13, rideTuple.f16);
    }

}

