package hpi.des.flink_tutorial.session2.solution;

import hpi.des.flink_tutorial.util.DateParser;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

public class Exercise8WindowProcessingOperatorSolution implements WindowFunction<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>, Tuple4<Integer, Double, Double, LocalDateTime>, Integer, TimeWindow> {
    @Override
    public void apply(Integer key, TimeWindow window, Iterable<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>> input,
                      Collector<Tuple4<Integer, Double, Double, LocalDateTime>> out) throws Exception {
        double avgTip;
        double sumTip = 0;
        int count = 0;
        LocalDateTime windowStart = DateParser.millisecondsToLocalDateTime(window.getStart());

        for(Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double> event : input){
            sumTip += event.f4;
            count ++;
        }

        if(count > 0) {
            avgTip = sumTip/count;
            out.collect(new Tuple4<>(key, avgTip, sumTip, windowStart));
        }

    }
}
