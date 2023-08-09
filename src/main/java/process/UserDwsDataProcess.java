package process;

import bean.User;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import source.CustomSource;
import utils.FlinkKafkaUtil;
import utils.GlobalConfUtil;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class UserDwsDataProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        FlinkKafkaUtil flinkKafkaUtil = new FlinkKafkaUtil();
        GlobalConfUtil conf = new GlobalConfUtil();
        DataStreamSource<User> userStream = env.addSource(new CustomSource(100, 0L));
        // 注意方法的泛型 assignTimestampsAndWatermarks()的部分方法官方已不建议使用

        SingleOutputStreamOperator<User> watermarkStream = userStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<User>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<User>() {
                                    @Override
                                    public long extractTimestamp(User user, long l) {
                                        return user.getTs();
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<String> resultDataStream = watermarkStream
                .keyBy(x -> x.getName())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new UserAggregateFunction(), new UserProcessWindowFunction());
        resultDataStream.print();
        env.execute("UserProcess");
    }
    static class UserAggregateFunction implements AggregateFunction<User, Map<String, Integer>, Map<String, Integer>> {

        @Override
        public Map<String, Integer> createAccumulator() {
            return new HashMap<String, Integer>();
        }

        @Override
        public Map<String, Integer> add(User user, Map<String, Integer> bufferMap) {
            Integer num = bufferMap.getOrDefault(user.getName(), 0);
            num = num + user.getNum();
            bufferMap.put(user.getName(), num);
            return bufferMap;
        }

        @Override
        public Map<String, Integer> getResult(Map<String, Integer> resultMap) {
            return resultMap;
        }

        @Override
        public Map<String, Integer> merge(Map<String, Integer> acc1, Map<String, Integer> acc2) {
            return null;
        }
    }

    static class UserProcessWindowFunction extends ProcessWindowFunction<Map<String, Integer>, String, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Map<String, Integer>> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Iterator<Map<String, Integer>> iterator = iterable.iterator();
            while (iterator.hasNext()) {
                collector.collect(
                        "start:" + start + "\t" +
                                "end:" + end + "\t" +
                                "key:" + key + "\t" +
                                "value:" + iterator.next().toString()
                );
            }
        }
    }

}
