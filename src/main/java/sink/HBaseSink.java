package sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import utils.GlobalConfUtil;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/*
* 一个HBase工具类
* 可以根据JavaBean的注解自动解析出rowKey和Column
* 注意：key获取目前是按照JavaBean对象的属性顺序拼接
* */

public class HBaseSink<T> extends RichSinkFunction<T> {

    private Connection connection;
    private Configuration hbaseConfig;
    private Table table;

    public HBaseSink() {


    }


    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws IOException {
        GlobalConfUtil conf = new GlobalConfUtil();
        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set(conf.getHbase_zookeeper_quorum_key(),conf.getHbase_zookeeper_quorum_value());
        hbaseConfig.set(conf.getHabse_zookeeper_property_clientPort_key(),conf.getHabse_zookeeper_property_clientPort_value());
        hbaseConfig.set(conf.getHbase_master_key(),conf.getHbase_master_value());

        connection = ConnectionFactory.createConnection(hbaseConfig);
        table = connection.getTable(TableName.valueOf(conf.getHbase_dim_table()));
    }

    @Override
    public void close() throws Exception {
        table.close();
        connection.close();
    }

    @Override
    public void invoke(T t, Context context) throws Exception {
        Class<?> tClass = t.getClass();
        Field[] fields = tClass.getDeclaredFields();
        // 存储rowKey的字符串和存储对象属性名称和值
        StringBuilder rowKeyBuilder = new StringBuilder();
        HashMap<byte[], byte[]> map = new HashMap<>();

        // 遍历获取rowKey拼接和属性名称、值的Map
        for (Field field : fields) {
            // Accessible:可访问的 反射获得的属性都是私有的 如果不设置会报错
            field.setAccessible(true);
            Object object = field.get(t);
            String fieldName = field.getName();
            Annotation[] annotations = field.getAnnotations();
            for (Annotation annotation : annotations) {
                String annotationName = annotation.annotationType().getName();
                if (annotationName.equals("annotation.RowKey")) {
                    rowKeyBuilder.append("-").append(object);
                } else if (annotationName.equals("annotation.Column")) {
                    // 序列化为字节码 减少map的空间占用
                    byte[] key = Bytes.toBytes(fieldName);
                    byte[] value = Bytes.toBytes(object.toString());
                    map.put(key, value);
                }
            }
        }
        // 除去StringBuilder前面的 "-" 字符串
        String rowKeyStr = rowKeyBuilder.substring(1);
        byte[] rowKey = Bytes.toBytes(rowKeyStr);
        byte[] family = Bytes.toBytes("family");
        Put put = new Put(rowKey);
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            put.addColumn(family, entry.getKey(), entry.getValue());
        }
        table.put(put);
    }
}
