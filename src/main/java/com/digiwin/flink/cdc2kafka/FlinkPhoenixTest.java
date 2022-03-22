/*
package com.digiwin.flink.cdc2kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

*/
/**
 * 主要功能：
 * 测试通过Flink调用Phoenix JDBC来创建Schema
 * 由于公司的Phoenix是CDH版本，期间jar包冲突过，整了很久
 * 具体可以参考
 * https://gujincheng.github.io/2022/03/01/Hbase%E5%AD%A6%E4%B9%A0%E7%AC%94%E8%AE%B0/
 *//*

@Deprecated
public class FlinkPhoenixTest {
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ddp3.hadoop:9092,ddp4.hadoop:9092,ddp5.hadoop:9092");
        properties.setProperty("group.id", "fink-test");
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        String inputTopic = "gjc_test";
        // Source
        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<String>(inputTopic, new MyKafkaDeserializationSchema(), properties);

        SingleOutputStreamOperator<Tuple2<String, Long>> aaa = env.addSource(consumer).flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        String[] arr = s.split(",");
                        for (String str : arr) {
                            collector.collect(new Tuple2<>(str, 1L));
                        }
                    }
                })
                .keyBy(x -> x.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    String phoenixServer = "jdbc:phoenix:ddp5.hadoop:2181";

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        if (phoenixServer == null) {
                            throw new RuntimeException("Config of phoenix server is null");
                        }
                        Class.forName(PHOENIX_DRIVER);
                        Properties props = new Properties();
                        props.put("phoenix.schema.isNamespaceMappingEnabled","true");
                        Connection connection = DriverManager.getConnection(phoenixServer, props);
                        connection.setAutoCommit(true);
                        String sql1 = "create schema IF NOT EXISTS \"aaa\"";
                        System.out.println("-----sql1"+sql1);
                        boolean execute = execute(connection, sql1);
                        System.out.println("-----execute1"+execute);
                    }


                    @Override
                    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>.Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                        out.collect(value);
                    }
                });

        aaa.print();
        env.execute();
    }
    private static boolean execute(Connection connection, String sql) throws Exception {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            boolean execute = preparedStatement.execute();
            return execute;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}*/
