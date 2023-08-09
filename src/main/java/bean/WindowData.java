package bean;

import net.schmizz.sshj.connection.channel.Window;

public class WindowData {
    private String windowStart;
    private String windowEnd;
    private String key;
    private Object value;

    public WindowData(String windowStart, String windowEnd, String key, Object value) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        String str = "windowStart:" + this.windowStart + "\t" +
                "windowEnd:" + this.windowEnd + "\t" +
                "key:" + this.key + "\t" +
                "value:" + this.value.toString();
        return str;
    }
}
