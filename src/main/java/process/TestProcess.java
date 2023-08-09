package process;


import akka.stream.impl.Concat;
import bean.User;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sink.ESSink;
import source.CustomSource;
import utils.GlobalConfUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/*
* 这个类是作者为了调试各个项目的 使用的测试类
* 整个项目不会引用该类 不会影响项目的运行
*
* */
public class TestProcess<T> {


    public String getInfo(T t) throws IllegalAccessException {
        Class<?> tClass = t.getClass();
        Field[] fields = tClass.getDeclaredFields();
        String rowKey = "";
        for (Field field : fields) {
            // 反射获取每个属性的名称 属性设置可访问 属性名称 属性值 属性的注解集合
            field.setAccessible(true);
            String fieldName = field.getName();
            Object value = field.get(t);
            Annotation[] annotations = field.getAnnotations();

            if (annotations.length == 0) {
                continue;
            } else {
                System.out.println(annotations.length);
                for (int index = 0; index < annotations.length; index++) {
                    String annotationsName = annotations[index].annotationType().getName();
                    System.out.println(annotationsName);
                    if (annotationsName.equals("annotation.RowKey")) {
                        rowKey = rowKey + "-" + fieldName;
                    } else if (annotationsName.equals("annotation.Column")) {

                    }
                }
            }
        }
        return "s";
    }

    public static void main(String[] args) throws Exception {
//        TestProcess<User> testProcess = new TestProcess<>();
//        String info = testProcess.getInfo(new User("zhangsan", "man", "click", 20, 1000L));
//        System.out.println(info);
//        StringBuilder builder = new StringBuilder();
//        builder.append("-").append("A").append("-").append("B");
//        String str = builder.toString().substring(1);
//        System.out.println(str);

        //TODO 测试ES Sink的测试程序
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
        DataStreamSource<User> userDataStreamSource = env.addSource(new CustomSource(100, 0L));
        userDataStreamSource.addSink(new ESSink<User>().getSink());
        env.execute();

//        HashMap<String, String> map = new HashMap<>();
//        map.put("key","value");
//        map.put("k1","v1");
//
//        ArrayList<String> list = new ArrayList<>();
//        list.add("person101");
//        list.add("person101");
//        list.add("person101");
//
//        System.out.println(map);
//        System.out.println(list);

//        GlobalConfUtil globalConfUtil = new GlobalConfUtil();
//        List<String> cluster = globalConfUtil.getElasticsearch_cluster_hostname();
//        for (String s:cluster){
//            System.out.println(s);
//        }
    }
}
