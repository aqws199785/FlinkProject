package utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import bean.CanalRowData;
import java.util.Properties;

public class KafkaUtil {
    private KafkaProducer kafkaProducer;
    private Properties properties;

    GlobalConfUtil globalConfUtil = new GlobalConfUtil();

    public KafkaUtil() {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, globalConfUtil.bootstrap_servers);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, globalConfUtil.batch_size);
        properties.put(ProducerConfig.ACKS_CONFIG, globalConfUtil.ack);
        properties.put(ProducerConfig.RETRIES_CONFIG, globalConfUtil.retries);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, globalConfUtil.client_id);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, globalConfUtil.key_serializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, globalConfUtil.value_serializer);
        this.kafkaProducer = new KafkaProducer(properties);
    }
    // 将数据发送到kafka
    public void send(CanalRowData canalRowData) {
        ProducerRecord producerRecord = new ProducerRecord(globalConfUtil.topic, canalRowData);
        kafkaProducer.send(producerRecord);
    }
}
