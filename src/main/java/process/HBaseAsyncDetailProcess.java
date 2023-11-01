package process;

import async.HBaseAsync;
import bean.User;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class HBaseAsyncDetailProcess {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        DataStreamSource<User> dataStream = env.fromElements(
                new User("李四", "man", "click", 4, 1000L)
        );

        SingleOutputStreamOperator<User> watermarks = dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<User>forBoundedOutOfOrderness(
                        Duration.ofSeconds(5)
                ).withTimestampAssigner(new SerializableTimestampAssigner<User>() {
                    @Override
                    public long extractTimestamp(User user, long l) {
                        return user.getTs();
                    }
                })
        );
        AsyncDataStream.unorderedWait(
                dataStream,
                new HBaseAsync(),
                5,
                TimeUnit.SECONDS,
                100
        );
    }
}
