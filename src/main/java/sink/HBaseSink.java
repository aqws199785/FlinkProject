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

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

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
        StringBuilder rowKeyBuilder = new StringBuilder();

        for (Field field : fields) {
            // Accessible:可访问的 反射获得的属性都是私有的 如果不设置会报错
            field.setAccessible(true);
            Object value = field.get(t);
            String fieldName = field.getName();
            Annotation[] annotations = field.getAnnotations();
            HashMap<String, byte[]> map = new HashMap<>();
            // 编写一个由注解获取主键字段和列字段 方法需要优化为执行一次


            for (Annotation annotation : annotations) {
                String annotationName = annotation.annotationType().getName();
                if (annotationName.equals("annotation.RowKey")) {
                    rowKeyBuilder.append("-").append(fieldName);
                } else if (annotationName.equals("annotation.Column")) {
                    byte[] bytes = Bytes.toBytes(value.toString());
                    map.put(fieldName, bytes);
                }
            }
            for (Map.Entry entry : map.entrySet()) {

            }

        }


        String key = rowKeyBuilder.toString().substring(1);

        byte[] rowKey = Bytes.toBytes("123");
        byte[] family = Bytes.toBytes("detail");
        Put put = new Put(rowKey);
        byte[] id = Bytes.toBytes("num");
        byte[] name = Bytes.toBytes("name");
        byte[] gender = Bytes.toBytes("gender");
        put.addColumn(family, id, Bytes.toBytes("zhansan"));
        table.put(put);

    }

    public static void fun(Annotation[] annotations) {

    }
}
