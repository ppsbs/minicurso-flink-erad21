package hpi.des.flink_tutorial.session1.solution;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple7;

import java.time.LocalDateTime;

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise2OperatorSolution implements FilterFunction<Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double>> {
    @Override
    public boolean filter(Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double> event) throws Exception {
        return event.f3 != null && event.f3 == 1 && event.f4 == 1;
    }
}
