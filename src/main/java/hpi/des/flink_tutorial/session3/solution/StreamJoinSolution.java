package hpi.des.flink_tutorial.session3.solution;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.StreamingFileSinkFactory;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

public class StreamJoinSolution {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamingFileSink<Tuple5<Long, Short, String, Float, Float>> sink = StreamingFileSinkFactory.newSink("/tmp/output_session_3a");

        // adds overhead but allows the part files from streaming file sink to be persisted
        env.enableCheckpointing(1000);
        env.setParallelism(4);

        DataStream<TaxiFare> fareStream = env.addSource(new TaxiFareGeneratorProcTime());
        DataStream<TaxiRide> rideStream = env.addSource(new TaxiRideGeneratorProcTime());

        rideStream
                // Exercise 11: joining TaxiRide and TaxiFare streams
                .join(fareStream)
                .where(new Exercise11WhereOperatorSolution())
                .equalTo(new Exercise11EqualToOperatorSolution())
                .window(Exercise11WindowJoinOperatorSolution.getWindow())
                .apply(new Exercise11WindowJoinProcessingOperatorSolution())

                .addSink(sink);

        env.execute("Exercise Session 3a");
    }

}
