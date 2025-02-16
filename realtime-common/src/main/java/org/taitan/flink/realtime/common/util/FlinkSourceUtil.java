package org.taitan.flink.realtime.common.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.taitan.flink.realtime.common.constant.Constant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/13 21:13
 * @Version 1.0.0
 * <p>
 * Describe:
 */
public class FlinkSourceUtil {

    public static KafkaSource<String> getKafkaSource(String groupId, String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(
                        // SimpleStringSchema 无法发序列化 null 值的数据，会直接报错
                        // 后续 DWD 层会向 kafka 中发送 null，不能使用 SimpleStringSchema
//                        new SimpleStringSchema(),
                        new DeserializationSchema<String>() {

                            @Override
                            public String deserialize(byte[] message) throws IOException {
                                // 解决 SimpleStringSchema 无法发序列化 null 值的数据，会直接报错
                                if (message != null) {
                                    return new String(message, StandardCharsets.UTF_8);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String nextElement) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return Types.STRING;
                            }
                        })
                .build();
    }

    public static MySqlSource<String> getMySqlSource(String databaseName, String tableName){
        // MySQL8.0+ 安全机制不允许下面这种远程密码连接，使用属性开启密码连接
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        return MySqlSource.<String>builder()
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .jdbcProperties(props)
                .databaseList(databaseName) // set captured database
                .tableList(databaseName + "." + tableName) // set captured table
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();
    }
}
