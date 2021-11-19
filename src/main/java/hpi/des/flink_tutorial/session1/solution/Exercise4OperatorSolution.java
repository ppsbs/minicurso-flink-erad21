package hpi.des.flink_tutorial.session1.solution;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise4OperatorSolution implements FlatMapFunction<Tuple8<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double>,
        Tuple9<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double, Double>> {
    @Override
    public void flatMap(Tuple8<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double> event,
                        Collector<Tuple9<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double, Double>> out) throws Exception {
        if(event.f2 != null && event.f2 > 0){
            double tipRatioPerPassenger = event.f7/event.f2;
            if(tipRatioPerPassenger > 0.01){
                out.collect(new Tuple9<>(event.f0, event.f1, event.f2, event.f4, event.f4, event.f5, event.f6,
                        event.f7, tipRatioPerPassenger));
            }
        }
    }
}
