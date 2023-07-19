package source;


import bean.User;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import java.util.Random;

/*
* 自定义数据来源
* batchNum 控制数据的批次量
* n  控制每批的数据量
* */

public class CustomSource implements ParallelSourceFunction<User> {
    Integer batchNum;
    Long producerInterval;
    public CustomSource(Integer batchNum,Long producerInterval) {
        this.batchNum = batchNum;
        this.producerInterval = producerInterval;
    }

    public void run(SourceContext<User> sourceContext) throws Exception {
        Random random = new Random();
        String[] nameArr = {"张三", "李四", "王五", "赵六", "周七"};
        String[] genderArr = {"男", "女"};
        String[] actionArr = {"click", "watch", "buy", "collect"};
        for (int i = 0; i < batchNum; i++) {
            int nameIndex = random.nextInt(nameArr.length);
            int genderIndex = random.nextInt(genderArr.length);
            int actionIndex = random.nextInt(actionArr.length);
            int action_num = random.nextInt(5);
            for (int n = 0; n < random.nextInt(10); n++) {
                sourceContext.collect(
                        new User(
                                nameArr[nameIndex],
                                genderArr[genderIndex],
                                actionArr[actionIndex],
                                action_num,
                                (long) (i + n) * 1000L
                        )
                );
            }
            Thread.sleep(producerInterval);

        };
    }

    public void cancel() {

    }
}

