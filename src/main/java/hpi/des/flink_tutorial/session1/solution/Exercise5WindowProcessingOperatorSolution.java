package hpi.des.flink_tutorial.session1.solution;

import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise5WindowProcessingOperatorSolution implements AllWindowFunction<Tuple9<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double, Double>,
        Double, TimeWindow> {

    @Override
    public void apply(TimeWindow window,
                      Iterable<Tuple9<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double, Double>> values,
                      Collector<Double> out) throws Exception {
        double maxRatio = -1.;
        for(Tuple9<LocalDateTime, LocalDateTime, Integer, Integer, Integer, Double, Double, Double, Double> event : values){
            if(maxRatio < event.f8){
                maxRatio = event.f8;
            }
        }
        out.collect(maxRatio);

    }
}
