package org.taitan.flink.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.taitan.flink.realtime.common.base.BaseSQLAPP;
import org.taitan.flink.realtime.common.constant.Constant;
import org.taitan.flink.realtime.common.util.SQLUtil;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/19 19:00
 * @Version 1.0.0
 * <p>
 * Describe: 交易域加购事务事实表
 * 提取加购操作生成加购表，写出到 Kafka 对应主题
 */
public class DwdTradeCartAdd  extends BaseSQLAPP {

    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013,4,"dwd_trade_cart_add");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务处理
        // 1. 读取 topic_db 数据
        createTopicDb(groupId, tableEnv);

        // 2. 筛选加购数据
        Table cartAddTable = filterCartAdd(tableEnv);

        // 3. 创建 kafkaSink 输出映射
        createKafkaSinkTable(tableEnv);

        // 4. 写出筛选的数据到对应的 kafka 主题
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }

    public void createKafkaSinkTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                "  id string,\n" +
                "  user_id string,\n" +
                "  sku_id string,\n" +
                "  cart_price string,\n" +
                "  sku_num string,\n" +
                "  sku_name string,\n" +
                "  is_checked string,\n" +
                "  create_time string,\n" +
                "  operate_time string,\n" +
                "  is_ordered string,\n" +
                "  order_time string,\n" +
                "  source_type string,\n" +
                "  source_id string,\n" +
                "  ts bigint\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    public Table filterCartAdd(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "  `data`['id'] id, \n" +
                "  `data`['user_id'] user_id, \n" +
                "  `data`['sku_id'] sku_id, \n" +
                "  `data`['cart_price'] cart_price, \n" +
                // 除了正常的加购，还有一种是在已有的加购基础上，比如已有加购商品为 1，现在在购物车页面加到了 3
                "  if(`type`='insert', `data`['sku_num'], cast(cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint) as string))   sku_num, \n" +
                "  `data`['sku_name'] sku_name, \n" +
                "  `data`['is_checked'] is_checked, \n" +
                "  `data`['create_time'] create_time, \n" +
                "  `data`['operate_time'] operate_time, \n" +
                "  `data`['is_ordered'] is_ordered, \n" +
                "  `data`['order_time'] order_time, \n" +
                "  `data`['source_type'] source_type, \n" +
                "  `data`['source_id'] source_id,\n" +
                "  ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='cart_info'\n" +
                // 过滤条件中会有两种加购方式，两种方式都要考虑到
                // 加购方式一：商品未在购物车中，首次加购，则 type='insert'
                // 加购方式二：商品已在购物车中，增量加购，则 type='update'
                "and (`type`='insert' or (\n" +
                "  `type`='update' and `old`['sku_num'] is not null \n" +
                "  and cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))");
    }
}
