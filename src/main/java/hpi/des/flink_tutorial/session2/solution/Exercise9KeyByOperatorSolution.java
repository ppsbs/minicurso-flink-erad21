package hpi.des.flink_tutorial.session2.solution;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;

import java.time.LocalDateTime;

public class Exercise9KeyByOperatorSolution implements KeySelector<Tuple4<Integer, Double, Double, LocalDateTime>, Integer> {

    @Override
    public Integer getKey(Tuple4<Integer, Double, Double, LocalDateTime> value) throws Exception {
        return value.f0;
    }
}
