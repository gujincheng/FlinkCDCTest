package com.digiwin.flink.cdc2kafka;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 主要测试FlinkCDC采集Mysql，包含断点续传，提交任务到yarn上
 */
public class Mysql2Kakfa {
    private static final Logger log = LoggerFactory.getLogger(Mysql2Kakfa.class);
    private static String hostName ;
    private static int yourPort;
    private static String dbName;
    private static String tableName;
    private static String userName;
    private static String password;

    private static boolean isTest = false;

    private static void parseArgs(String[] args) {
        log.info("begin export data! args={}", Arrays.toString(args));
        List<String> argsLeft = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("-isTest".equals(args[i])) {
                isTest = true;
            } else {
                argsLeft.add(args[i]);
            }
        }

        if (argsLeft.size() % 2 != 0) {
            log.error("输入参数个数异常，请检查！");
            System.exit(1);
        }
        //判断错误参数的标记，如果有错误参数，异常退出
        int error = 0;
        for (int i = 0; i < argsLeft.size(); i = i + 2) {
            if ("-hostName".equals(argsLeft.get(i))) {
                hostName = argsLeft.get(i + 1);
            } else if ("-yourPort".equals(argsLeft.get(i))) {
                yourPort = Integer.parseInt(argsLeft.get(i + 1));
            } else if ("-dbName".equals(argsLeft.get(i))) {
                dbName = argsLeft.get(i + 1);
            } else if ("-tableName".equals(argsLeft.get(i))) {
                tableName = argsLeft.get(i + 1);
            } else if ("-userName".equals(argsLeft.get(i))) {
                userName = argsLeft.get(i + 1);
            } else if ("-password".equals(argsLeft.get(i))) {
                password = argsLeft.get(i + 1);
            }
        }

        if (error > 0) {
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        //parseArgs(args);
        hostName = "golden-01";
        yourPort = 3306;
        dbName = "test";
        tableName = "test.gjc_test_binlog";
        userName = "root";
        password = "123456";
        System.out.println("hostName:" + hostName
                + ",port:" + yourPort
                + ",dbName:" + dbName
                + ",tableName:" + tableName
                + ",userName:" + userName
                + ",password:" + password
        );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置ck的状态后端
        //env.setStateBackend(new HashMapStateBackend());
        //设置系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        //env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置checkpoint存储目录
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://flink/checkpoint/cdc/gjc_test_Mysql2Kakfa");


        //env.setParallelism(1);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(hostName)
                .port(yourPort)
                .databaseList(dbName) // set captured database
                .tableList(tableName) // set captured table
                .username(userName)
                .password(password)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        // enable checkpoint
        //env.enableCheckpointing(3000);
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                //.setParallelism(4)
                .print(); // use parallelism 1 for sink to keep message ordering
        env.execute("Print MySQL Snapshot + Binlog");

    }
}
