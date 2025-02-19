package org.taitan.flink.realtime.common.util;

import org.taitan.flink.realtime.common.constant.Constant;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/18 18:54
 * @Version 1.0.0
 * <p>
 * Describe:
 */
public class SQLUtil {

    public static String getKafkaSourceSQL(String topicName,String groupId){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS +"',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }
    public static String getKafkaTopicDb(String groupId){
        return "CREATE TABLE topic_db (\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `data` map<STRING, STRING>,\n" +
                "  `old` map<STRING, STRING>,\n" +
                "  `ts` bigint,\n" +
                // 执行 LookUp Join 要求提供一个处理时间
                "  proc_time as PROCTIME(), \n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts,3), \n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ") " + getKafkaSourceSQL(Constant.TOPIC_DB, groupId);
    }

    public static String getKafkaSinkSQL(String topicName){
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS +"',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    /**
     * 获取 upsert kafka 的连接   创建表格的语句最后一定要声明主键
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSQL(String topicName){
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + topicName + "',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS +"',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }
}
