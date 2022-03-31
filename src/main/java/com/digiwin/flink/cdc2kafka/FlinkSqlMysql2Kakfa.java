package com.digiwin.flink.cdc2kafka;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
public class FlinkSqlMysql2Kakfa {
    private static final Logger log = LoggerFactory.getLogger(FlinkSqlMysql2Kakfa.class);
    private static String hostName ;
    private static int yourPort;
    private static String dbName;
    private static String tableName;
    private static String userName;
    private static String password;


    public static void main(String[] args) throws Exception {
        hostName = "150.158.190.192";
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
        //配置ck的状态后端
        env.setStateBackend(new HashMapStateBackend());
        //设置系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置checkpoint存储目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://golden-02:9000/flink/checkpoint/cdc/gjc_test_Mysql2Kakfa");
        env.enableCheckpointing(3000);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
        tenv.executeSql(
                "CREATE TABLE gjc_test_binlog ( " +
                        " id INTEGER, " +
                        " a INTEGER, " +
                        " t_modified STRING ," +
                        "PRIMARY KEY(id) NOT ENFORCED" +
                        ") WITH ( " +
                        " 'connector' = 'mysql-cdc', " +
                        " 'hostname' = '150.158.190.192', " +
                        " 'port' = '3306', " +
                        " 'username' = 'root', " +
                        " 'password' = 'Gjc123!@#', " +
                        " 'database-name' = 'test', " +
                        " 'table-name' = 'gjc_test_binlog' " +
                        //" 'scan.incremental.snapshot.enabled'='false' " +
                       // " 'debezium.include.schema.changes'='true'" +
                ") ");
        //报错：The 'OPTIONS' hint is allowed only when the config option 'table.dynamic-table-options.enabled' is set to true
        //Table table = tenv.sqlQuery("select * from gjc_test_binlog /*+ OPTIONS('server-id'='5401-5404') */  ");
        Table table = tenv.sqlQuery("select id,a,t_modified  from gjc_test_binlog ");
        tenv.toRetractStream(table, Row.class).print();

        env.execute();

    }
}
