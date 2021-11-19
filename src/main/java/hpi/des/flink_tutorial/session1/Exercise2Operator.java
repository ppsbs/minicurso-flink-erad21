package hpi.des.flink_tutorial.session1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple7;

import java.time.LocalDateTime;

// implement here the class of your operator. Check TransformSourceStreamOperator.java for an example. Don't change the name
// of the class because it is used in the testing.
public class Exercise2Operator implements FilterFunction<Tuple7<LocalDateTime, LocalDateTime,
        Integer, Integer, Integer, Double, Double>> {

    @Override
    public boolean filter(Tuple7<LocalDateTime, LocalDateTime, Integer, Integer, Integer,
            Double, Double> dataItem) throws Exception {
        return dataItem != null && dataItem.f3 != null && dataItem.f4 != null
                && dataItem.f3 == 1 && dataItem.f4 == 1;
    }

}
