package interfaces;

/*
 * 编写一个接口
 * 这个接口是为了之后使用kafka的protobuf格式的value序列化方式做准备
 * canal解析的数据要实现该接口才能写入到kafka
 * */
public interface ProtobufAble {
    byte[] toBytes();
}
