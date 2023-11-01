package async;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import utils.GlobalConfUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseAsync<T> extends RichAsyncFunction<T, Object> {

    private Connection connection;
    private org.apache.hadoop.conf.Configuration hbaseConfig;
    private Table table;

    @Override
    public void open(Configuration parameters) throws Exception {
        GlobalConfUtil conf = new GlobalConfUtil();
        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set(conf.getHbase_zookeeper_quorum_key(), conf.getHbase_zookeeper_quorum_value());
        hbaseConfig.set(conf.getHabse_zookeeper_property_clientPort_key(), conf.getHabse_zookeeper_property_clientPort_value());
        hbaseConfig.set(conf.getHbase_master_key(), conf.getHbase_master_value());
        connection = ConnectionFactory.createConnection(hbaseConfig);
        table = connection.getTable(TableName.valueOf(conf.getHbase_dim_table()));
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<Object> resultFuture) throws Exception {
        Class<?> tClass = t.getClass();
        Field[] fields = tClass.getDeclaredFields();
        String rowKey = "";
        HashMap<String, Object> map = new HashMap<>();
        for (Field field : fields) {
            field.setAccessible(true);
            Annotation[] annotations = field.getAnnotations();
            String fieldName = field.getName();
            Object fieldValue = field.get(t);
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().getName().equals("annotation.RowKey")) {
                    rowKey = rowKey + "-" + fieldValue;
                }
            }
            map.put(fieldName, fieldValue);

        }
        rowKey = rowKey.substring(1);

        Get get = new Get(Bytes.toBytes(rowKey));
        Get family = get.addFamily(Bytes.toBytes("family"));
        Result result = table.get(family);
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("family"));
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            String key = Bytes.toString(entry.getKey());
            String value = Bytes.toString(entry.getValue());
            map.put(key, value);
        }


    }

    @Override
    public void close() throws Exception {
        table.close();
        connection.close();
    }

    @Override
    public void timeout(T input, ResultFuture<Object> resultFuture) throws Exception {
        System.out.println("异步IO失败:" + input.toString());
    }
}
