package bean;

import annotation.Column;
import annotation.Id;
import annotation.RowKey;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class User implements Serializable {
    @RowKey
    @Column
    Integer num;
    @Id
    @RowKey
    @Column
    String name;
    @Column
    String gender;
    @Column
    String action;
    @Id
    @Column
    Long ts;

    public User() {
    }

    public User(String name, String gender, String action, Integer num, Long ts) {
        this.name = name;
        this.gender = gender;
        this.action = action;
        this.num = num;
        this.ts = ts;
    }
}
