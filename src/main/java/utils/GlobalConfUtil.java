package utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;
import lombok.Setter;


@Getter @Setter
public class GlobalConfUtil {
    String bootstrap_servers;

    String zookeeper_servers;
    String group_id;
    String enable_auto_commit;
    String auto_commit_interval_ms;
    String auto_offset_reset;
    String key_serializer;
    String value_serializer;
    String kafka_topic;

    String mysql_server_url;
    String mysql_server_ip;
    String mysql_server_port;
    String mysql_server_database;
    String mysql_server_username;
    String mysql_server_password;


    public GlobalConfUtil() {
        Config config = ConfigFactory.load();
        this.bootstrap_servers = config.getString("bootstrap.servers");
        this.zookeeper_servers = config.getString("zookeeper.servers");
        this.group_id = config.getString("group.id");
        this.enable_auto_commit = config.getString("enable.auto.commit");
        this.auto_commit_interval_ms = config.getString("auto.commit.interval.ms");
        this.auto_offset_reset = config.getString("auto.offset.reset");
        this.key_serializer = config.getString("key.serializer");
        this.value_serializer = config.getString("key.deserializer");

        this.mysql_server_url = config.getString("mysql.server.url");
        this.mysql_server_ip = config.getString("mysql.server.ip");
        this.mysql_server_port = config.getString("mysql.server.port");
        this.mysql_server_database = config.getString("mysql.server.database");
        this.mysql_server_username = config.getString("mysql.server.username");
        this.mysql_server_password = config.getString("mysql.server.password");
        this.kafka_topic = config.getString("kafka.topic");
    }

    public static void main(String[] args) {
        GlobalConfUtil confUtil = new GlobalConfUtil();
        System.out.println(confUtil.toString());
        System.out.println(confUtil.mysql_server_password);
    }

}
