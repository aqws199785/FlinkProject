package process;

import bean.CanalRowData;
import com.alibaba.fastjson2.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import utils.FlinkKafkaUtil;
import utils.GlobalConfUtil;

public class KafkaDwdDataProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);

        FlinkKafkaUtil flinkKafkaUtil = new FlinkKafkaUtil();
        GlobalConfUtil globalConfUtil = new GlobalConfUtil();
        FlinkKafkaConsumer011<CanalRowData> canalRowDataFlinkKafkaConsumer = flinkKafkaUtil.kafkaConsumer(globalConfUtil.getTopic());
        DataStreamSource<CanalRowData> canalRowDataDataStream = env.addSource(canalRowDataFlinkKafkaConsumer);
        SingleOutputStreamOperator<String> JsonDataStream = canalRowDataDataStream.map(canalRowData -> JSON.toJSONString(canalRowData));

        FlinkKafkaProducer011<String> kafkaProducer = flinkKafkaUtil.kafkaProducer(globalConfUtil.getDwd_user_action_mysql());
        JsonDataStream.addSink(kafkaProducer);
        env.execute();
    };
}
