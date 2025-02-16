package org.taitan.flink.realtime.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.taitan.flink.realtime.common.constant.Constant;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/16 20:06
 * @Version 1.0.0
 * <p>
 * Describe:
 */
public class FlinkSinkUtil {

    public static KafkaSink<String> getKafkaSink(String topicName){
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer( new KafkaRecordSerializationSchemaBuilder<String>()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("taitan-" + topicName + System.currentTimeMillis())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }
}
