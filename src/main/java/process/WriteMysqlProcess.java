package process;

import bean.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sink.MysqlSink;
import source.CustomSource;

public class WriteMysqlProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        String sql = "insert into user (user_name,gender,action,pv,ts) value (?,?,?,?,?)";
        Integer batchNum = 100;
        Long producerInterval = 0L;
        DataStreamSource<User> streamSource = env.addSource(new CustomSource(batchNum,producerInterval));

        streamSource.addSink(new MysqlSink<User>(sql));


        env.execute("mysql");
    }

}
