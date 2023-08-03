package annotation;


import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RowKey {
/*
* 设置一个注解 作用在字段上 运行时生效
* 用于反射时获取注解 筛选需要写入的主键
* */
}
