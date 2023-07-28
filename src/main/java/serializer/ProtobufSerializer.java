package serializer;
import interfaces.ProtobufAble;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class ProtobufSerializer implements  Serializer<ProtobufAble> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String s, ProtobufAble protobufAble) {
        return protobufAble.toBytes();
    }

    @Override
    public void close() {

    }
}
