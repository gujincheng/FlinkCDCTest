package com.digiwin.flink.cdc2kafka;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.apache.flink.core.fs.Path;
import java.util.concurrent.TimeUnit;

public class Flink2HDFS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);

        System.out.println("*********** hdfs ***********************");

        StreamingFileSink<String> hdfsSink = StreamingFileSink
                .forRowFormat(new Path("hdfs://golden-02:9000/tmp"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                DefaultRollingPolicy.builder()
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                        .withMaxPartSize(1024 * 1024 * 1024)
                        .build())
                .build();

        source.addSink(hdfsSink);
        env.execute("write data to hdfs");
    }
}
