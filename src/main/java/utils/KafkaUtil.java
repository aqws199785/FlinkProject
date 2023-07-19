package utils;

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

public class KafkaUtil {

    KafkaProperties kafkaProperties = new KafkaProperties();

    public KafkaUtil() {
    }

    public FlinkKafkaConsumer011<String> KafkaConsumer(String topic) {
        GlobalConfUtil confUtil = new GlobalConfUtil();
        FlinkKafkaConsumer011<String> KafkaConsumer = new FlinkKafkaConsumer011<String>(
                topic,
                new SimpleStringSchema(),
                kafkaProperties.getKafkaProperties()
        );
        return KafkaConsumer;
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
