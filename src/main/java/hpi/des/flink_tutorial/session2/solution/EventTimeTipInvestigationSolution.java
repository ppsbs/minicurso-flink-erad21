package hpi.des.flink_tutorial.session2.solution;

import hpi.des.flink_tutorial.session1.TransformSourceStreamOperator;
import hpi.des.flink_tutorial.session2.PreprocessStream;
import hpi.des.flink_tutorial.util.DateParser;
import hpi.des.flink_tutorial.util.InputFile;
import hpi.des.flink_tutorial.util.TaxiRideTuple;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class EventTimeTipInvestigationSolution {

    public static Duration exercise6WatermarkInterval(){
        // your code here
        return Duration.ofMinutes(10);
    }

    public static SerializableTimestampAssigner<TaxiRideTuple> exercise6GetTimestampFromTaxiRideTuple(){
        // your code here
        return new SerializableTimestampAssigner<TaxiRideTuple>() {
            @Override
            public long extractTimestamp(TaxiRideTuple event, long recordTimestamp) {
                return DateParser.localDateTimeToMilliseconds(event.f1);
            }
        };
    }

    public static void main(String[] args) throws Exception {

        // Exercise 6a and 6b: watermark and timestamp
        WatermarkStrategy<TaxiRideTuple> watermarkStrategy = WatermarkStrategy.<TaxiRideTuple>forBoundedOutOfOrderness(exercise6WatermarkInterval())
                .withTimestampAssigner(exercise6GetTimestampFromTaxiRideTuple());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<TaxiRideTuple> taxiRideStream = env.readTextFile(InputFile.getInputFilePath())
            .flatMap(new TransformSourceStreamOperator())
            .assignTimestampsAndWatermarks(watermarkStrategy);

        taxiRideStream
                /* setup stream */
                .flatMap(new PreprocessStream())

                // Exercise 7: partitions the data based on the value of the pick-up location id.
                .keyBy(new Exercise7OperatorSolution())

                // Exercise 8: sum and average of tip ratios per passenger in a given region per hour.
                .window(Exercise8WindowOperatorSolution.getWindow())
                .apply(new Exercise8WindowProcessingOperatorSolution())

                // Exercise 9: get avg and sum tip in a region per day.
                .keyBy(new Exercise9KeyByOperatorSolution())
                .window(Exercise9WindowOperatorSolution.getWindow())
                .reduce(new Exercise9WindowProcessingOperatorSolution())

                // Exercise 10: get the best pick-up region of all day.
                .windowAll(Exercise10WindowOperatorSolution.getWindow())
                .aggregate(new Exercise10WindowProcessingOperatorSolution())

                // sink
                .print();

        env.execute();
    }
}
