package hpi.des.flink_tutorial.session1;

import hpi.des.flink_tutorial.util.InputFile;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TipInvestigation {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> sourceStream = env.readTextFile(InputFile.getInputFilePath());

        sourceStream.flatMap(new TransformSourceStreamOperator())

                // exercise 1
                // .operator1(new Exercise1Operator())
                // operator1 could be a map, filter, flatMap, etc
                // Read TransformSourceStreamOperator for more insights
                .map(new Exercise1Operator())

                // exercise 2
                // .operator2(new Exercise2Operator())
                // operator2 could be a map, filter, flatMap, etc
                // Read TransformSourceStreamOperator for more insights
                .filter(new Exercise2Operator())

                // exercise 3
                // .operator3(new Exercise3Operator())
                // operator3 could be a map, filter, flatMap, etc

                // exercise 4
                // .operator4(new Exercise4Operator())
                // operator4 could be a map, filter, flatMap, etc

                // exercise 5
                // .operator5(new Exercise4Operator())
                // operator5 could be a map, filter, flatMap, etc

                .print();



        env.execute("Exercise Session 1");
    }
}
