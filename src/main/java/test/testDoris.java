package test;

import bean.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import sink.Doris;
import source.CustomSource;

public class testDoris {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomSource(10, 500L));
        userDataStreamSource.print();
        SingleOutputStreamOperator<GenericRowData> streamOperator = userDataStreamSource.map(
                user -> {
                    GenericRowData rowData = new GenericRowData(5);
                    rowData.setField(0, StringData.fromString(user.getName()));
                    rowData.setField(1, StringData.fromString(user.getGender()));
                    rowData.setField(2, StringData.fromString(user.getAction()));
                    rowData.setField(3, user.getNum());
                    rowData.setField(4, user.getTs());
                    return rowData;
                }
        );
        Doris doris = new Doris();
//        streamOperator.addSink(doris.getSink());
        DataStreamSource<String> source = env.fromElements("{\"id\": \"66\", \"city\": \"6\", \"name\": \"pengyuyan\",\"pv\": \"6\"}");
        source.addSink(doris.getJsonSink());
        env.execute();

    }
}
