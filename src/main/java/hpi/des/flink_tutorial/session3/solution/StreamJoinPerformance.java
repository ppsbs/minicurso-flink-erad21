package hpi.des.flink_tutorial.session3.solution;

import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiFare;
import hpi.des.flink_tutorial.session3.generator.datatypes.TaxiRide;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiFareGeneratorProcTime;
import hpi.des.flink_tutorial.session3.generator.sources.TaxiRideGeneratorProcTime;
import hpi.des.flink_tutorial.util.InputFile;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class StreamJoinPerformance {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamingFileSink<Tuple7<Long, Short, String, Float, Float, Long, Long>> sink = StreamingFileSink.forRowFormat(
                new Path("/tmp/output_session_3b"),
                new SimpleStringEncoder<Tuple7<Long, Short, String, Float, Float, Long, Long>>("UTF-8")).build();

        DataStream<Tuple2<TaxiFare, Long>> fareStream = env.addSource(new TaxiFareGeneratorProcTime())
                .map(new MapFunction<TaxiFare, Tuple2<TaxiFare, Long>>() {
                    @Override
                    public Tuple2<TaxiFare, Long> map(TaxiFare value) throws Exception {
                        return new Tuple2<>(value, System.currentTimeMillis());
                    }
                });

        DataStream<TaxiRide> rideStream = env.addSource(new TaxiRideGeneratorProcTime());

        rideStream
                .map(new MapFunction<TaxiRide, Tuple2<TaxiRide, Long>>() {
                        @Override
                        public Tuple2<TaxiRide, Long> map(TaxiRide value) throws Exception {
                            return new Tuple2<>(value, System.currentTimeMillis());
                        }
                    })
                .join(fareStream)
                .where(new KeySelector<Tuple2<TaxiRide, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple2<TaxiRide, Long> value) throws Exception {
                        return value.f0.rideId;
                    }
                })
                .equalTo(new KeySelector<Tuple2<TaxiFare, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple2<TaxiFare, Long> value) throws Exception {
                        return value.f0.rideId;
                    }
                })
                
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .apply(new JoinFunction<Tuple2<TaxiRide, Long>, Tuple2<TaxiFare, Long>, Tuple7<Long, Short, String, Float, Float, Long, Long>>() {
                    @Override
                    public Tuple7<Long, Short, String, Float, Float, Long, Long> join(Tuple2<TaxiRide, Long> first, Tuple2<TaxiFare, Long> second) throws Exception {
                        Tuple7<Long, Short, String, Float, Float, Long, Long> result = new Tuple7<>();
                        result.f0 = first.f0.rideId;
                        result.f1 = first.f0.passengerCnt;
                        result.f2 = second.f0.paymentType;
                        result.f3 = second.f0.totalFare;
                        result.f4 = second.f0.tip;
                        result.f5 = first.f1 < second.f1 ? first.f1 : second.f1;
                        result.f6 = System.currentTimeMillis();

                        return result;
                    }
                })
                .addSink(sink);

        env.execute("Exercise Session 3b");
    }

}
