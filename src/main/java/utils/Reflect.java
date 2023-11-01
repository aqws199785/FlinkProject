package utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Reflect<T> {

    // 传入任意一个对象 获取对象的属性 返回一个Map
    public Map<String, Map<String, Object>> getObjectAllStats(T t, Boolean isAllStats) throws IllegalAccessException {
        Map<String, Map<String, Object>> map = new HashMap<>();
        Class<?> tClass = t.getClass();
        Field[] fields = tClass.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String fieldName = field.getName();
            Object fieldValue = field.get(t);
            Annotation[] annotations = field.getAnnotations();
            HashMap<String, Object> statsMap = new HashMap<>();

            // 将Annotation数组转换为 Annotation type name集合
            ArrayList<String> annotationList = new ArrayList<>();
            for (Annotation annotation : annotations) {
                String annotationName = annotation.annotationType().getName();
                annotationList.add(annotationName);
            }
            statsMap.put("value", fieldValue);
            statsMap.put("annotation", annotationList);
            map.put(fieldName, statsMap);
        }
        return map;
    }
}
