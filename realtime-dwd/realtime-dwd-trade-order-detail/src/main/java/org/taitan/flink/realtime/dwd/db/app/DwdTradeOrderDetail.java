package org.taitan.flink.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.taitan.flink.realtime.common.base.BaseSQLAPP;
import org.taitan.flink.realtime.common.constant.Constant;
import org.taitan.flink.realtime.common.util.SQLUtil;

import java.time.Duration;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/19 19:16
 * @Version 1.0.0
 * <p>
 * Describe: 交易域下单事务事实表
 * 关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表的 insert 操作，形成下单明细表，写入 Kafka 对应主题
 */
public class DwdTradeOrderDetail extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024,4,"dwd_trade_order_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {

        // 在 flinkSQL 中使用 join 一定要添加状态的存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        // 核心业务编写
        // 1. 读取 topic_db 数据
        createTopicDb(groupId, tableEnv);

        // 2. 筛选订单详情表数据
        filterOd(tableEnv);

        // 3. 筛选订单信息表
        filterOi(tableEnv);

        // 4. 筛选订单详情活动关联表
        filterOda(tableEnv);

        // 5. 筛选订单详情优惠券关联表
        filterOdc(tableEnv);

        // 6. 将四张表格 join 合并
        Table joinTable = getJoinTable(tableEnv);

        // 7. 写出到 kafka 中
        // 一旦使用了left join 会产生撤回流  此时如果需要将数据写出到 kafka
        // 不能使用一般的 kafka sink  必须使用 upsert kafka
        createUpsertKafkaSink(tableEnv);

        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }

    /**
     * 写出到 upsert kafka
     * @param tableEnv
     */
    public void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL +" (\n" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  create_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint,\n" +
                // 因为上一步使用了 left join, 所以会产生回撤流
                // 回撤流写 kafka，需要使用 upsert kafka 连接
                // upsert kafka 在连接时，要求在建表最后声明一个主键
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    /**
     * 将四张表格 join 合并
     * @param tableEnv
     * @return
     */
    public Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts \n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on oda.id = od.id\n" +
                "left join order_detail_coupon odc\n" +
                "on odc.id = od.id ");
    }

    /**
     * 订单详情优惠券关联表
     * @param tableEnv
     */
    public void filterOdc(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type`='insert'");

        tableEnv.createTemporaryView("order_detail_coupon",odcTable);
    }

    /**
     * 订单详情活动关联表
     * @param tableEnv
     */
    public void filterOda(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("select \n" +
                "  `data`['order_detail_id'] id, \n" +
                "  `data`['activity_id'] activity_id, \n" +
                "  `data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity",odaTable);
    }

    /**
     * 订单信息表
     * @param tableEnv
     */
    public void filterOi(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['user_id'] user_id, \n" +
                "  `data`['province_id'] province_id \n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='insert'");

        tableEnv.createTemporaryView("order_info",oiTable);
    }

    /**
     * 订单详情表
     * @param tableEnv
     */
    public void filterOd(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['order_id'] order_id, \n" +
                "  `data`['sku_id'] sku_id, \n" +
                "  `data`['sku_name'] sku_name, \n" +
                "  `data`['order_price'] order_price, \n" +
                "  `data`['sku_num'] sku_num, \n" +
                "  `data`['create_time'] create_time, \n" +
                "  `data`['split_total_amount'] split_total_amount, \n" +
                "  `data`['split_activity_amount'] split_activity_amount, \n" +
                "  `data`['split_coupon_amount'] split_coupon_amount, \n" +
                "  ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail",odTable);
    }
}
