package sink;

import annotation.RowKey;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/*
* 一个HBase工具类
* 可以根据JavaBean的注解自动解析出rowKey和Column
* */

public class HBaseSink<T> extends RichSinkFunction<T> {

    private Connection connection;
    private Configuration hbaseConfig;
    private Table table;

    public HBaseSink() {


    }


    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws IOException {
        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", "person101,person102,person103");
        hbaseConfig.set("habse.zookeeper.property.clientPort", "2181");
        hbaseConfig.set("hbase.master", "person101:60020");
        connection = ConnectionFactory.createConnection(hbaseConfig);
        table = connection.getTable(TableName.valueOf("dim_user_info"));
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
//
//        String key = rowKeyBuilder.toString().substring(1);
//        byte[] rowKey = Bytes.toBytes("123");
//        byte[] family = Bytes.toBytes("detail");
//        Put put2 = new Put(rowKey);
//        byte[] id = Bytes.toBytes("num");
//        byte[] name = Bytes.toBytes("name");
//        byte[] gender = Bytes.toBytes("gender");
//        put2.addColumn(family, id, Bytes.toBytes("zhansan"));
//        table.put(put2);
    }
}
