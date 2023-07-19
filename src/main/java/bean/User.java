package bean;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class User {
    String name;
    String gender;
    String action;
    Integer num;
    Long ts;

    public User(){}

    public User(String name, String gender, String action, Integer num, Long ts) {
        this.name = name;
        this.gender = gender;
        this.action = action;
        this.num = num;
        this.ts = ts;
    }
}
