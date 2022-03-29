package com.digiwin.flink.cdc2kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 主要测试FlinkCDC采集Mysql，包含断点续传，提交任务到yarn上
 */
public class FlinkSqlServer2Kakfa {
    private static final Logger log = LoggerFactory.getLogger(FlinkSqlServer2Kakfa.class);
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
        tableName = "test.gjc_test_binlog";
        userName = "sa";
        password = "Gjc123!@#";
        System.out.println("hostName:" + hostName
                + ",port:" + yourPort
                + ",dbName:" + dbName
                + ",tableName:" + tableName
                + ",userName:" + userName
                + ",password:" + password
        );
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
        tenv.executeSql(
                "CREATE TABLE gjc_test_binlog ( " +
                        " id INTEGER, " +
                        " a INTEGER, " +
                        " t_modified STRING " +
                        ") WITH ( " +
                        " 'connector' = 'sqlserver-cdc', " +
                        " 'hostname' = '192.168.233.129', " +
                        " 'port' = '1433', " +
                        " 'username' = 'sa', " +
                        " 'password' = 'Gjc123!@#', " +
                        " 'database-name' = 'test', " +
                        " 'schema-name' = 'dbo', " +
                        " 'table-name' = 'gjc_test_binlog' " +
                ") ");

        Table table = tenv.sqlQuery("select id,a,t_modified  from gjc_test_binlog ");
        tenv.toRetractStream(table, Row.class).print();
        env.execute();

    }
}
