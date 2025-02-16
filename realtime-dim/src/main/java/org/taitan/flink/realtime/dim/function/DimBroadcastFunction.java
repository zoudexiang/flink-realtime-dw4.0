package org.taitan.flink.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.taitan.flink.realtime.common.bean.TableProcessDim;
import org.taitan.flink.realtime.common.util.JdbcUtil;

import java.util.HashMap;
import java.util.List;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/15 17:04
 * @Version 1.0.0
 * <p>
 * Describe:
 */
public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {

    /**
     * 此处的 hashMap 为了解决在主流和配置流连接后，在处理的过程中(本类的计算逻辑)，由于 create hbase table 的流程会相对缓慢
     * 先到的一部分主流数据，有可能获取不到状态(broadcastState), 所以在 open 中预加载初始化维度表信息，修复 bug
     */
    public HashMap<String,TableProcessDim> hashMap ;
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 预加载初始的维度表信息
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDims = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dim", TableProcessDim.class, true);
        hashMap = new HashMap<>();
        for (TableProcessDim tableProcessDim : tableProcessDims) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    /**
     * 处理广播流数据
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcessDim value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 读取广播状态
        BroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        // 将配置表信息作为一个维度表的标记 写到广播状态
        String op = value.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(value.getSourceTable());
            // 同步删除hashMap中初始化加载的配置表信息
            hashMap.remove(value.getSourceTable());
        } else {
            tableProcessState.put(value.getSourceTable(), value);
        }
    }

    /**
     * 处理主流数据
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        // 读取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = ctx.getBroadcastState(broadcastState);
        // 查询广播状态  判断当前的数据对应的表格是否存在于状态里面
        String tableName = value.getString("table");
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);
        // 如果是数据到的太早  造成状态为空
        if (tableProcessDim == null){
            tableProcessDim = hashMap.get(tableName);
        }
        if (tableProcessDim != null) {
            // 状态不为空  说明当前一行数据是维度表数据
            out.collect(Tuple2.of(value, tableProcessDim));
        }
    }
}

