package annotation;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Id {
    /*
    * 设置一个注解 运行时生效
    * 用于反射时获取注解 筛选需要设置为Id的列
    * */
}
