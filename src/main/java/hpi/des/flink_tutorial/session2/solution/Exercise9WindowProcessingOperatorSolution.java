package hpi.des.flink_tutorial.session2.solution;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.time.LocalDateTime;

public class Exercise9WindowProcessingOperatorSolution implements ReduceFunction<Tuple4<Integer, Double, Double, LocalDateTime>> {
    @Override
    public Tuple4<Integer, Double, Double, LocalDateTime> reduce(Tuple4<Integer, Double, Double, LocalDateTime> value1, Tuple4<Integer, Double, Double, LocalDateTime> value2) throws Exception {
        return new Tuple4<>(value1.f0,
                Double.max(value1.f1, value2.f1),
                value1.f2 + value2.f2,
                value1.f1 > value2.f1 ? value1.f3 : value2.f3);
    }
}
