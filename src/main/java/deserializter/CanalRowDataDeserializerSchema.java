package deserializter;

import bean.CanalRowData;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import java.io.IOException;


/*
* 编写一个自定义的反序列化类
* 调用CanalRowData(byte[] bytes) 构造方法解析二进制字节数组
* 可以参考 SimpleStringSchema 的 deserialize SimpleStringSchema实现了DeserializationSchema
* CanalRowDataDeserializerSchema继承了AbstractDeserializationSchema 继承DeserializationSchema的子类
* */
public class CanalRowDataDeserializerSchema extends AbstractDeserializationSchema<CanalRowData> {
    @Override
    public CanalRowData deserialize(byte[] bytes) throws IOException {
        return new CanalRowData(bytes);
    }
}
