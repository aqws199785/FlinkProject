package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import utils.GlobalConfUtil;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/*
* 定义一个mysql sink
* 满足解析任意数据类型
* */

public class MysqlSink<T> extends RichSinkFunction<T> {

    String sql;
    Connection connection;
    PreparedStatement preparedStatement;

    public MysqlSink() {
    }

    public MysqlSink(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        GlobalConfUtil globalConfUtil = new GlobalConfUtil();
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(
                globalConfUtil.getMysql_server_url(),
                globalConfUtil.getMysql_server_username(),
                globalConfUtil.getMysql_server_password()
        );
        preparedStatement = connection.prepareStatement(sql);
    }

    public void invoke(T value, Context context) throws Exception {
        Class<?> valueClass = value.getClass();
        Field[] fields = valueClass.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            field.setAccessible(true);

            Object fieldValue = field.get(value);
            preparedStatement.setObject(i + 1, fieldValue);
        }
        preparedStatement.execute();
    }

    @Override
    public void close() throws Exception {
        preparedStatement.close();
        connection.close();
    }
}
