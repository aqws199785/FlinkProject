package utils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

/*
* 创建一个获取kafka配置的公共类
* 注意:如下使用消费者的是ConsumerConfig
* 生产者的需要使用ProducerConfig
*
* */

public class KafkaProperties {
    public KafkaProperties(){

    }

    public Properties getKafkaProperties(){
        GlobalConfUtil globalConf = new GlobalConfUtil();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,globalConf.bootstrap_servers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,globalConf.group_id);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,globalConf.enable_auto_commit);
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,globalConf.auto_commit_interval_ms);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,globalConf.auto_offset_reset);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,globalConf.key_serializer);
        return properties;
    }
}
