package utils;

import bean.CanalRowData;
import deserializter.CanalRowDataDeserializerSchema;
import lombok.val;
import lombok.var;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/*
 * 创建一个kafka的工具类
 * (1) 从kafka消费数据
 * (2) 向kafka发送数据
 * */

public class FlinkKafkaUtil {

    KafkaProperties kafkaProperties = new KafkaProperties();

    public FlinkKafkaUtil() {
    }

    // 用以消费发送到kafka的日志数据
    public FlinkKafkaConsumer011<String> KafkaConsumer(String topic) {
        FlinkKafkaConsumer011<String> KafkaConsumer = new FlinkKafkaConsumer011<String>(
                topic,
                new SimpleStringSchema(),
                kafkaProperties.getKafkaProperties()
        );
        return KafkaConsumer;
    }

    // 用以消费发送到kafka的业务数据
    public FlinkKafkaConsumer011<CanalRowData> kafkaConsumer(String topic) {
        FlinkKafkaConsumer011<CanalRowData> kafkaConsumer = new FlinkKafkaConsumer011<CanalRowData>(
                topic,
                new CanalRowDataDeserializerSchema(),
                kafkaProperties.getKafkaProperties()
        );
        return kafkaConsumer;
    }

    public FlinkKafkaProducer011<String> kafkaProducer(String topic) {
        KafkaProperties properties = new KafkaProperties();
        Properties kafkaProperties = properties.getKafkaProperties();
        FlinkKafkaProducer011 kafkaProducer = new FlinkKafkaProducer011(
                topic,
                new KeyedSerializationSchemaWrapper(new SimpleStringSchema()),
                kafkaProperties
        );
        return kafkaProducer;
    }
}
