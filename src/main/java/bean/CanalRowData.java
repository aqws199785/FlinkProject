package bean;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.HashMap;

import com.alibaba.fastjson2.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import interfaces.ProtobufAble;
import protobuf.CanalModel;

@Getter
@Setter
public class CanalRowData implements ProtobufAble {

    private String logfileName;
    private Long logfileOffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;

    // 将Map对象转换为CanalRowData对象 写入kafka时会使用
    public CanalRowData(Map map) {
        if (map.size() > 0) {
            this.logfileName = map.get("logfileName").toString();
            this.logfileOffset = Long.parseLong(map.get("logfileOffset").toString());
            this.executeTime = Long.parseLong(map.get("executeTime").toString());
            this.schemaName = map.get("schemaName").toString();
            this.tableName = map.get("tableName").toString();
            this.eventType = map.get("eventType").toString();
            this.columns = (Map<String, String>) map.get("columns");
        }
    }

    // 将byte数组装换位CanalRowData对象  导出kafka数据是会使用
    // 需要注意proto文件中定义的数据类型 否则会出现类型不匹配错误
    public CanalRowData(byte[] bytes) {
        try {
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfileName = rowData.getLogfileName();
            this.logfileOffset = rowData.getLogfileOffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchemaName();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();
            this.columns = new HashMap<String,String>();
            this.columns.putAll(rowData.getColumnsMap());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }


    @Override
    public byte[] toBytes() {
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.logfileName);
        System.out.println(this);
        builder.setLogfileOffset(this.logfileOffset);
        builder.setExecuteTime(this.executeTime);
        builder.setSchemaName(this.schemaName);
        builder.setTableName(this.tableName);
        builder.setEventType(this.eventType);
        if (this.columns!=null) {
            builder.putAllColumns(this.columns);
        }
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
