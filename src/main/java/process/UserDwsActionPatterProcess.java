package process;

import bean.User;
import javafx.util.Pair;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import source.CustomSource;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserDwsActionPatterProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        DataStreamSource<User> streamSource = env.addSource(new CustomSource(100, 0L));
        SingleOutputStreamOperator<User> watermarksStream = streamSource.assignTimestampsAndWatermarks(
                WatermarkStrategy.<User>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5)
                ).withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                    @Override
                    public long extractTimestamp(User user, long l) {
                        return user.getTs();
                    }
                })
        );
        KeyedStream<User, String> userKeyedStream = watermarksStream.keyBy(user -> {
            Pair pair = new Pair(user.getName(), user.getGender());
            return pair.toString();
        });
        Pattern<User, User> pattern = Pattern.<User>begin("click")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User user) throws Exception {
                        return user.getAction().equals("click");
                    }
                })
                .followedBy("collect")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User user) throws Exception {
                        return user.getAction().equals("collect");
                    }
                })
                .followedBy("buy")
                .where(new SimpleCondition<User>() {
                    @Override
                    public boolean filter(User user) throws Exception {
                        return user.getAction().equals("buy");
                    }
                    // 多少时间内出现该模式数据
                }).within(Time.seconds(30));
        PatternStream<User> patternStream = CEP.pattern(userKeyedStream, pattern);
        OutputTag<Object> userOutputTag = new OutputTag<Object>("TimeOut") {
        };
        SingleOutputStreamOperator<Object> streamOperator = patternStream.select(
                userOutputTag,
                new PatternTimeoutFunction<User, Object>() {
                    @Override
                    public Object timeout(Map<String, List<User>> map, long timeoutTimestamp) throws Exception {
                        // Map 存储的是 <pattern_name,List<User>>
                        return map;
                    }
                },
                new PatternSelectFunction<User, Object>() {
                    @Override
                    public Object select(Map<String, List<User>> map) throws Exception {
                        return (HashMap) map;
                    }
                }
        );
        streamOperator.print();
        streamOperator.getSideOutput(userOutputTag).print("timeout");
        env.execute("cep");
    }
}
