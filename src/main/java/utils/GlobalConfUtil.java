package utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;
import lombok.Setter;


@Getter
@Setter
public class GlobalConfUtil {

    // kafka配置的变量
    String bootstrap_servers;
    String zookeeper_servers;
    String group_id;
    String enable_auto_commit;
    String auto_commit_interval_ms;
    String auto_offset_reset;
    String key_serializer;
    String key_deserializer;
    String value_serializer;

    String batch_size;
    String ack;
    String retries;
    String client_id;

    String topic;
    String dwd_user_action_mysql;
    String dwd_shop_sell_mysql;
    String dwd_user_click_log;


    // mysql 相关配置
    String mysql_server_url;
    String mysql_server_ip;
    int mysql_server_port;
    String mysql_server_database;
    String mysql_server_username;
    String mysql_server_password;

    // canal 相关配置
    String canal_destination;
    String canal_username;
    String canal_password;
    String canal_subscribe;


    public GlobalConfUtil() {
        Config config = ConfigFactory.load();
        this.bootstrap_servers = config.getString("bootstrap.servers");
        this.zookeeper_servers = config.getString("zookeeper.servers");
        this.group_id = config.getString("group.id");
        this.enable_auto_commit = config.getString("enable.auto.commit");
        this.auto_commit_interval_ms = config.getString("auto.commit.interval.ms");
        this.auto_offset_reset = config.getString("auto.offset.reset");

        this.key_serializer = config.getString("key.serializer");
        this.key_deserializer = config.getString("key.deserializer");
        this.value_serializer = config.getString("value.serializer");

        this.batch_size = config.getString("batch_size");
        this.ack = config.getString("ack");
        this.retries = config.getString("retries");
        this.client_id = config.getString("client_id");

        this.topic = config.getString("topic");
        this.dwd_user_action_mysql = config.getString("dwd_user_action_mysql");
        this.dwd_shop_sell_mysql=config.getString("dwd_shop_sell_mysql");
        this.dwd_user_click_log=config.getString("dwd_user_click_log");

        this.mysql_server_url = config.getString("mysql.server.url");
        this.mysql_server_ip = config.getString("mysql.server.ip");
        this.mysql_server_port = config.getInt("mysql.server.port");
        this.mysql_server_database = config.getString("mysql.server.database");
        this.mysql_server_username = config.getString("mysql.server.username");
        this.mysql_server_password = config.getString("mysql.server.password");

        this.canal_destination = config.getString("canal.destination");
        this.canal_username = config.getString("canal.username");
        this.canal_password = config.getString("canal.password");
        this.canal_subscribe = config.getString("canal.subscribe");
    }

    public static void main(String[] args) {
        GlobalConfUtil confUtil = new GlobalConfUtil();
        System.out.println(confUtil.toString());
        System.out.println(
                confUtil.zookeeper_servers + "\n" +
                        confUtil.canal_username + "\n" +
                        confUtil.canal_password + "\n" +
                        confUtil.canal_destination + "\n" +
                        confUtil.canal_subscribe
        );
    }

}
