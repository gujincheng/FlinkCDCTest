package com.digiwin.flink.cdc2kafka;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 主要测试FlinkCDC采集Mysql，包含断点续传，提交任务到yarn上
 */
public class FlinkSqlMysql2Kakfa {
    private static final Logger log = LoggerFactory.getLogger(FlinkSqlMysql2Kakfa.class);
    private static String hostName ;
    private static int yourPort;
    private static String dbName;
    private static String tableName;
    private static String userName;
    private static String password;


    public static void main(String[] args) throws Exception {
        hostName = "golden-01";
        yourPort = 3306;
        //dbName = "[a-zA-Z\\d]+_test";
        //tableName = "[a-zA-Z\\d]+_test.gjc_test_binlog_[0-9][0-9]";
        dbName = "test";
        tableName = "test.gjc_test_binlog_noprimary";
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
        EnvironmentSettings Settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, Settings);

        tenv.executeSql(
                "CREATE TABLE gjc_test_binlog ( " +
                        " id INTEGER, " +
                        " a INTEGER, " +
                        " t_modified STRING " +
                        ") WITH ( " +
                        " 'connector' = 'mysql-cdc', " +
                        " 'hostname' = 'golden-01', " +
                        " 'port' = '3306', " +
                        " 'username' = 'root', " +
                        " 'password' = '123456', " +
                        " 'database-name' = 'test', " +
                        " 'table-name' = 'gjc_test_binlog_noprimary', " +
                        " 'scan.incremental.snapshot.enabled'='false'" +
                ") ");
        Table table = tenv.sqlQuery("select * from gjc_test_binlog");
        tenv.toRetractStream(table, Row.class).print();
        env.execute();

    }
}
