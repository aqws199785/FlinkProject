package sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import java.util.concurrent.TimeUnit;

public class HDFSSink {
    String path;
    String prefix;
    String suffix;

    public HDFSSink() {
    }

    public HDFSSink(String path, String prefix, String suffix) {
        this.path = path;
        this.prefix = prefix;
        this.suffix = suffix;
    }

    public FileSink write() {
        OutputFileConfig fileConfig = OutputFileConfig
                .builder()
                .withPartPrefix(prefix)
                .withPartSuffix(suffix)
                .build();
        DateTimeBucketAssigner dateTimeBucketAssigner = new DateTimeBucketAssigner("yyyy-MM-dd HH");

        FileSink fileSystemSink = FileSink
                .forRowFormat(new Path(path), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(dateTimeBucketAssigner)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build()
                )
                .withOutputFileConfig(fileConfig)
                .build();
        return fileSystemSink;
    }
}
