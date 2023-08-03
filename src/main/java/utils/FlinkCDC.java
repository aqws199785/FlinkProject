package utils;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class FlinkCDC {
    private StreamExecutionEnvironment env;

    public FlinkCDC(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public DataStreamSource mysqlSource() {
        GlobalConfUtil conf = new GlobalConfUtil();
        /*
         * tableList 需要填写 database.table 防止databaseList中出现重复的表
         * StartupOptions
         * StartupOptions.initial()          扫描表后切换的最新的的数据位置
         * StartupOptions.earliest()         从最早的数据开始,可以用来恢复数据
         * StartupOptions.latest()           从最新的数据开始
         * StartupOptions specificOffset()   从指定文件的指定位置读取
         * StartupOptions.timestamp()        binlog日志有时间戳,从指定的时间戳来读取数据
         * */

        // 使用官方的反序列化方式需要注意泛型 MySqlSource.<String>builder()
        MySqlSource build = MySqlSource.<String>builder()
                .hostname(conf.mysql_server_ip)
                .port(conf.mysql_server_port)
                .databaseList(conf.mysql_server_database)
                .tableList("study.user")
                .username(conf.mysql_server_username)
                .password(conf.mysql_server_password)
                .startupOptions(StartupOptions.latest())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        env.setParallelism(4);
        DataStreamSource mysqlSource = env.fromSource(build, WatermarkStrategy.noWatermarks(), "mysql source");
        return mysqlSource;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkCDC flinkCDC = new FlinkCDC(env);
        DataStreamSource dataStreamSource = flinkCDC.mysqlSource();
        dataStreamSource.print();
        env.execute();
    }
}
