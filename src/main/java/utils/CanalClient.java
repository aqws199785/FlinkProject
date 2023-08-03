package utils;

import bean.CanalRowData;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.List;

/*
 * CanalClient程序
 * 用于解析订阅的binlog日志
 * 注意使用的canal与mysql的兼容性
 * canal与fastjson2兼容性
 * */

public class CanalClient {
    private static final Integer BATCH_SIZE = 5 * 1024;
    private CanalConnector canalConnector;
    private GlobalConfUtil globalConfUtil;


    public CanalClient() {
        globalConfUtil = new GlobalConfUtil();
        canalConnector = new CanalConnectors().newClusterConnector(
                globalConfUtil.zookeeper_servers,
                globalConfUtil.canal_destination,
                globalConfUtil.canal_username,
                globalConfUtil.canal_password
        );
    }

    public void start() {
        canalConnector.connect();
        canalConnector.subscribe(globalConfUtil.canal_subscribe);
        canalConnector.rollback();

        while (true) {
            Message message = canalConnector.getWithoutAck(BATCH_SIZE);
            long id = message.getId();
            int size = message.getEntries().size();
            if (id == -1 || size == 0) {
                continue;
            } else {
                HashMap map = new HashMap();

                for (Entry entry : message.getEntries()) {
                    if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                        continue;
                    } else {
                        CanalEntry.Header header = entry.getHeader();
                        String logfileName = header.getLogfileName();
                        long logfileOffset = header.getLogfileOffset();
                        long executeTime = header.getExecuteTime();
                        String schemaName = header.getSchemaName();
                        String tableName = header.getTableName();
                        String eventType = header.getEventType().toString().toLowerCase();

                        map.put("logfileName", logfileName);
                        map.put("logfileOffset", logfileOffset);
                        map.put("executeTime", executeTime);
                        map.put("schemaName", schemaName);
                        map.put("tableName", tableName);
                        map.put("eventType", eventType);

                        HashMap columnDataMap = new HashMap();
                        ByteString storeValue = entry.getStoreValue();
                        try {
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                            for (CanalEntry.RowData rowData : rowDataList) {
                                if (eventType.equals("insert") || eventType.equals("update")) {
                                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                                        columnDataMap.put(column.getName(), column.getValue());
                                    }
                                } else if (eventType.equals("delete")) {
                                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                                        columnDataMap.put(column.getName(), column.getValue());
                                    }
                                } else {
                                    continue;
                                }
                            }
                            map.put("columns", columnDataMap);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
                CanalRowData canalRowData = new CanalRowData(map);
                KafkaUtil kafkaUtil = new KafkaUtil();
                System.out.println(canalRowData.getColumns());
                kafkaUtil.send(canalRowData);
            }
        }
    }

    public static void main(String[] args) {
        CanalClient canalClient = new CanalClient();
        canalClient.start();
    }
}

/*
 * Message 数据结构
 * Message[id=-1,entries=[],raw=false,rawEntries=[]]
 *
 * RowData 信息
 * afterColumns {
  index: 2
  sqlType: 4
  name: "num"
  isKey: false
  updated: true
  isNull: false
  value: "5"
  mysqlType: "int"
}
 *
 * */