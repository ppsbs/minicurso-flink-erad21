package hpi.des.flink_tutorial.session2.solution;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;

import java.time.LocalDateTime;

public class Exercise7OperatorSolution implements KeySelector<Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double>, Integer> {
    @Override
    public Integer getKey(Tuple5<Integer, LocalDateTime, Integer, LocalDateTime, Double> event) throws Exception {
        return event.f0;
    }
}
