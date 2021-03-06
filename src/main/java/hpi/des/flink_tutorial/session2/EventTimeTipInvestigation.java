package hpi.des.flink_tutorial.session2;

import hpi.des.flink_tutorial.session1.TransformSourceStreamOperator;
import hpi.des.flink_tutorial.util.DateParser;
import hpi.des.flink_tutorial.util.InputFile;
import hpi.des.flink_tutorial.util.TaxiRideTuple;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class EventTimeTipInvestigation {

    public static Duration exercise6WatermarkInterval(){
        // your code here
        return null;
    }

    public static SerializableTimestampAssigner<TaxiRideTuple> exercise6GetTimestampFromTaxiRideTuple(){
        // notice that we are instantiating a new class which follows the SerializableTimestampAssigner interface
        return new SerializableTimestampAssigner<TaxiRideTuple>() {
            @Override
            public long extractTimestamp(TaxiRideTuple event, long recordTimestamp) {
                // your code here
                return -1L;
            }
        };
    }

    public static void main(String[] args) throws Exception {
        // exercise 6 - implement methods exercise6WatermarkInterval and exercise6GetTimestampFromTaxiRideTuple used in the following call
        WatermarkStrategy<TaxiRideTuple> watermarkStrategy = WatermarkStrategy.<TaxiRideTuple>forBoundedOutOfOrderness(exercise6WatermarkInterval())
                .withTimestampAssigner(exercise6GetTimestampFromTaxiRideTuple());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRideTuple> taxiRideStream = env.readTextFile(InputFile.getInputFilePath())
                .flatMap(new TransformSourceStreamOperator())
                .assignTimestampsAndWatermarks(watermarkStrategy);

        taxiRideStream
                .flatMap(new PreprocessStream())
                // exercise 7
                // exercise 8
                // exercise 9
                // exercise 10
                .print();

        env.execute("Exercise Session 2");
    }

}
