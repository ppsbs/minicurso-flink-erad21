package hpi.des.flink_tutorial.session1.solution;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise3OperatorSolution implements FlatMapFunction<Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double>,
        Tuple8<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double>> {
    @Override
    public void flatMap(Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double> event,
                        Collector<Tuple8<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double>> out) throws Exception {
        if(event.f5 != null && event.f5 >= 0 && event.f6 != null && event.f6 > 0){
            double tipRatio = event.f5/event.f6;
            out.collect(new Tuple8<>(event.f0, event.f1, event.f2, event.f3, event.f4, event.f5, event.f6, tipRatio));
        }
    }
}
