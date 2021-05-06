package hpi.des.flink_tutorial.session2.solution;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

public class Exercise10WindowProcessingOperatorSolution implements AggregateFunction<Tuple4<Integer, Double, Double, LocalDateTime>, Tuple4<Integer, Double, Double, LocalDateTime>, Tuple4<Integer, Double, Double, LocalDateTime>>{

    @Override
    public Tuple4<Integer, Double, Double, LocalDateTime> createAccumulator() {
        return null;
    }

    @Override
    public Tuple4<Integer, Double, Double, LocalDateTime> add(Tuple4<Integer, Double, Double, LocalDateTime> value, Tuple4<Integer, Double, Double, LocalDateTime> accumulator) {
        return accumulator == null || value.f2 > accumulator.f2 ? value : accumulator;
    }

    @Override
    public Tuple4<Integer, Double, Double, LocalDateTime> getResult(Tuple4<Integer, Double, Double, LocalDateTime> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple4<Integer, Double, Double, LocalDateTime> merge(Tuple4<Integer, Double, Double, LocalDateTime> a, Tuple4<Integer, Double, Double, LocalDateTime> b) {
        return a.f2 > b.f2 ? a : b;
    }
}

// another possibility
//public class Exercise10WindowProcessingOperatorSolution implements AllWindowFunction<Tuple4<Integer, Double, Double, LocalDateTime>, Tuple4<Integer, Double, Double, LocalDateTime>, TimeWindow> {
//    @Override
//    public void apply(TimeWindow window, Iterable<Tuple4<Integer, Double, Double, LocalDateTime>> values,
//                      Collector<Tuple4<Integer, Double, Double, LocalDateTime>> out) throws Exception {
//        Tuple4<Integer, Double, Double, LocalDateTime> best = null;
//        for(Tuple4<Integer, Double, Double, LocalDateTime> event : values){
//            if(best == null || best.f2 < event.f2){
//                best = event;
//            }
//        }
//
//        if(best != null){
//            out.collect(new Tuple4<>(best.f0, best.f1, best.f2, best.f3));
//        }
//    }
//}