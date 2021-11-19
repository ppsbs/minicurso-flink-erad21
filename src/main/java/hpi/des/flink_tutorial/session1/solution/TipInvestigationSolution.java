package hpi.des.flink_tutorial.session1.solution;

import hpi.des.flink_tutorial.session1.TransformSourceStreamOperator;
import hpi.des.flink_tutorial.util.InputFile;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TipInvestigationSolution {



    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> stream = env.readTextFile(InputFile.getInputFilePath());
        stream.flatMap(new TransformSourceStreamOperator())
            /* Exercise 1: remove the fields that we are not going to use. Leave only pickup time, dropoff time,
             passenger count, rate type, payment type, tip, and total. */
            .map(new Exercise1OperatorSolution())

            // Exercise 2: keep only standard fares paid with credit card
            .filter(new Exercise2OperatorSolution())

            // Execise 3: add tip ratio if tip and total fare are valid
            .flatMap(new Exercise3OperatorSolution())

            // Exercise 4: add ratio percentage of tip per passenger if ration > 1%
            .flatMap(new Exercise4OperatorSolution())

            // Exercise 5: get the maxRatio in the last 1 s with 200ms slide of processing time
            .windowAll(Exercise5WindowOperatorSolution.getWindow())
            .apply(new Exercise5WindowProcessingOperatorSolution())

            // sink
            .print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
