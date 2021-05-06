package hpi.des.flink_tutorial.session3.solution;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

// implement here the class of your operator. For window operators, you must modify the method getWindow and use it
// in your stream processing job.
public class Exercise11WindowJoinOperatorSolution {
    public static WindowAssigner<Object, TimeWindow> getWindow(){
        return TumblingProcessingTimeWindows.of(Time.seconds(1));
    }
}
