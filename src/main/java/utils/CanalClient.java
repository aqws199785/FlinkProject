package utils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.xmlbeans.impl.xb.xsdschema.Public;

import java.util.Properties;

public class CanalClient {
    private static final Integer BATCH_SIZE = 5 * 1024;
    private CanalConnector canalConnector;
    private GlobalConfUtil globalConfUtil;


    public CanalClient() {
        globalConfUtil = new GlobalConfUtil();
        canalConnector = new CanalConnectors().newClusterConnector(
                globalConfUtil.zookeeper_servers,
                "example",
                "canal",
                "canal"
        );
    }

    public void start() {
        canalConnector.connect();
        canalConnector.rollback();
        canalConnector.subscribe("*");

        while (true){
            Message message = canalConnector.getWithoutAck(BATCH_SIZE);
            System.out.println(message);
        }
    }
}
