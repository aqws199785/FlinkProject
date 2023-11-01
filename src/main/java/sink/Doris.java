package sink;

import com.ibm.icu.impl.ValidIdentifiers;
import lombok.var;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

public class Doris {
    /*
    *
    * 需要你指定字段名称数组
    * 指定字段数据类型数组
    * */
    public SinkFunction getSink() {

        String[] fields = {"name", "gender", "action", "action_num", "ts"};
//        DataType[] types = {DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.BIGINT(), DataTypes.STRING()};
        LogicalType[] types = {new VarCharType(), new VarCharType(), new VarCharType(), new IntType(), new BigIntType()};
        DorisExecutionOptions dorisExecutionOptions = new DorisExecutionOptions.Builder()
                .setBatchIntervalMs(2000L)
                .setEnableDelete(false)
                .setMaxRetries(3)
                .setBatchSize(100)
                .build();
        DorisOptions dorisOptions = new DorisOptions.Builder()
                .setFenodes("person101:8030")
                .setUsername("root")
                .setPassword("root")
                .setTableIdentifier("study.user")
                .build();

        return DorisSink.sink(fields, types, dorisExecutionOptions, dorisOptions);
    }

    public SinkFunction getJsonSink() {

        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("strip_outer_array", "true");
        DorisReadOptions dorisReadOptions = DorisReadOptions.builder().build();

        DorisExecutionOptions dorisExecutionOptions = new DorisExecutionOptions.Builder()
                .setBatchIntervalMs(2000L)
                .setEnableDelete(false)
                .setMaxRetries(3)
                .setBatchSize(100)
                .setStreamLoadProp(properties)
                .build();
        DorisOptions dorisOptions = new DorisOptions.Builder()
                .setFenodes("person101:8030")
                .setUsername("root")
                .setPassword("root")
                .setTableIdentifier("study.json")
                .build();
        return DorisSink.sink(dorisReadOptions, dorisExecutionOptions, dorisOptions);
    }

}
