package process;


import akka.stream.impl.Concat;
import bean.User;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

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

    public static void main(String[] args) throws IllegalAccessException {
        TestProcess<User> testProcess = new TestProcess<>();
//        String info = testProcess.getInfo(new User("zhangsan", "man", "click", 20, 1000L));
//        System.out.println(info);
        StringBuilder builder = new StringBuilder();
        builder.append("-").append("A").append("-").append("B");
        String str = builder.toString().substring(1);
        System.out.println(str);
    }
}
