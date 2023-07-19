package process;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import sink.HDFSSink;
import utils.GlobalConfUtil;
import utils.KafkaUtil;

public class KafkaProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        KafkaUtil kafkaUtil = new KafkaUtil();
        GlobalConfUtil globalConfUtil = new GlobalConfUtil();
        FlinkKafkaConsumer011 kafkaConsumer = kafkaUtil.KafkaConsumer(globalConfUtil.getKafka_topic());

        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaConsumer);
        HDFSSink hdfsSink = new HDFSSink("hdfs://person101:8020/user", "user", ".txt");
        FileSink fileSink = hdfsSink.write();
        kafkaDataStream.sinkTo(fileSink);

        env.execute();
    }
}
