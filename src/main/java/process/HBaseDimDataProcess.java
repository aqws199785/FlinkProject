package process;

import bean.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sink.HBaseSink;
import source.CustomSource;

public class HBaseDimDataProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        DataStreamSource<User> userDataStream = env.addSource(new CustomSource(100, 1000L));
        userDataStream.addSink(new HBaseSink());
        env.execute();
    }
}
