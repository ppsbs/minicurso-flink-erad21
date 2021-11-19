package hpi.des.flink_tutorial.session1.solution;

import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

// implement here the class of your operator. For window operators, you must modify the method getWindow and use it
// in your stream processing job.
public class Exercise5WindowOperatorSolution {

    public static WindowAssigner<Object, TimeWindow> getWindow(){
        return SlidingProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(200));
    }
}