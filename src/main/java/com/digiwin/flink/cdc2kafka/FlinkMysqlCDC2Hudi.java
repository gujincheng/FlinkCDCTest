package com.digiwin.flink.cdc2kafka;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkMysqlCDC2Hudi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //配置ck的状态后端
        env.setStateBackend(new HashMapStateBackend());
        //设置系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置checkpoint存储目录
        env.getCheckpointConfig().setCheckpointStorage("hdfs://golden-02:9000/flink/checkpoint/sql/flinkMysqlCDC2Hudi");
        env.enableCheckpointing(3000);
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);
        tenv.executeSql(
                "CREATE TABLE users_source_mysql ( " +
                        "uuid STRING PRIMARY KEY NOT ENFORCED," +
                        "name STRING," +
                        "age int," +
                        "ts timestamp(3)," +
                        "part STRING" +
                        ") WITH ( " +
                        " 'connector' = 'mysql-cdc', " +
                        " 'hostname' = 'golden-02', " +
                        " 'port' = '3306', " +
                        " 'username' = 'root', " +
                        " 'password' = 'Gjc123!@#', " +
                        " 'debezium.snapshot.mode'='initial'," +
                        " 'server-time-zone'= 'Asia/Shanghai'," +
                        " 'database-name' = 'test', " +
                        " 'table-name' = 'users_source_mysql' " +
                        ") ");
        tenv.executeSql(
                "CREATE TABLE t2(\n" +
                        "  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,\n" +
                        "  name VARCHAR(10),\n" +
                        "  age INT,\n" +
                        "  ts timestamp(3),\n" +
                        "  part VARCHAR(20)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "  'connector' = 'hudi',\n" +
                        "  'path' = 'hdfs://golden-02:9000/user/hive/warehouse/hudi.db/t2',\n" +
                        "  'table.type' = 'MERGE_ON_READ',\n" +
                        "  'hoodie.datasource.write.recordkey.field'= 'uuid', \n" +
                        "  'write.precombine.field'= 'ts',\n" +
                        "  'write.tasks' = '1',\n" +
                        "  'write.rate.limit' = '2000',\n" +
                        "  'compaction.tasks' = '1',\n" +
                        "  'compaction.async.enabled' = 'true',\n" +
                        "  'compaction.trigger.strategy' = 'num_commits',\n" +
                        "  'compaction.delta_commits' = '1',\n" +
                        "  'changleog.enabled' = 'true',\n" +
                        "  'read.streaming.enabled'= 'true',\n" +
                        "  'read.streaming.check-interval'= '3',\n" +
                        "  'hive_sync.enable' = 'true',     -- Required. To enable hive synchronization\n" +
                        "  'hive_sync.mode' = 'hms',        -- Required. Setting hive sync mode to hms, default jdbc\n" +
                        "  'hive_sync.metastore.uris' = 'thrift://golden-02:9083', -- Required. The port need set on hive-site.xml\n" +
                        "  'hive_sync.jdbc_url' = 'jdbc://hive2://golden-02:10000',\n" +
                        "  'hive_sync.table'='t2',                  -- required, hive table name\n" +
                        "  'hive_sync.db'='hudi',\n" +
                        "  'hive_sync.username' = 'gujincheng',\n" +
                        "  'hive_sync.password' = '980071',\n" +
                        "  'hive_sync.support_timstamp' = 'true'\n" +
                        ")"
        );
        tenv.executeSql("insert into t2 select uuid,name,age,ts,part  from users_source_mysql");

        //上面的tenv.executeSql就是触发执行的操作，不需要再使用env.execute()再次触发一次
        //如果使用env.execute()再次触发一次，那么提交任务的时候就会启动2个job，提交到yarn上，会注册2个applicationid


    }
}
