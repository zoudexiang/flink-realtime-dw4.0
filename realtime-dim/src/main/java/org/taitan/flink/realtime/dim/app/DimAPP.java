package org.taitan.flink.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.taitan.flink.realtime.common.base.BaseAPP;
import org.taitan.flink.realtime.common.bean.TableProcessDim;
import org.taitan.flink.realtime.common.constant.Constant;
import org.taitan.flink.realtime.common.util.FlinkSourceUtil;
import org.taitan.flink.realtime.common.util.HBaseUtil;
import org.taitan.flink.realtime.dim.function.DimBroadcastFunction;
import org.taitan.flink.realtime.dim.function.DimHBaseSinkFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/13 22:11
 * @Version 1.0.0
 * <p>
 * Describe:
 */
public class DimAPP extends BaseAPP {

    public static void main(String[] args) {

       new DimAPP().start(10001,4,"dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 核心业务逻辑
        // 1. 对 ods 读取的原始数据进行数据清洗
        SingleOutputStreamOperator<JSONObject> jsonObjStream = transfer(stream);

        // 2. 使用 flinkCDC 读取监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        // 使用 FlinkCDC 读取数据时，并行度设置为 1，否则会出问题
        DataStreamSource<String> mysqlSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
//        mysqlSource.print();

        // 3. 在 HBase 中创建维度表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHBaseTable(mysqlSource).setParallelism(1);

//        createTableStream.print();

        // 4. 做成广播流
        // 广播状态的 key 用于判断是否是维度表，value 用于补充信息写出到 HBase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTableStream.broadcast(broadcastState);

        // 5. 连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectionStream(jsonObjStream, broadcastState, broadcastStateStream);

        // 6. 筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = filterColumn(dimStream);

        // 7. 写出到 HBase
        filterColumnStream.addSink(new DimHBaseSinkFunction());
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumn(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDim dim = value.f1;
                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObj.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });
    }

    /**
     * 主流 connect 配置流
     * @param jsonObjStream        Maxwell 读取的主流数据
     * @param broadcastState       FlinkCDC 读取的配置流数据的 MapStateDescriptor
     * @param broadcastStateStream FlinkCDC 读取的配置流数据
     * @return
     */
    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream(SingleOutputStreamOperator<JSONObject> jsonObjStream,
                                                                                            MapStateDescriptor<String, TableProcessDim> broadcastState,
                                                                                            BroadcastStream<TableProcessDim> broadcastStateStream) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjStream.connect(broadcastStateStream);

        return connectStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1);
    }

    /**
     * 基于 FlinkCDC 读取到的配置流数据，在 HBase 中创建各个维表
     * @param mysqlSource
     * @return
     */
    public SingleOutputStreamOperator<TableProcessDim> createHBaseTable(DataStreamSource<String> mysqlSource) {

        // 使用 flatMap 算子还需要使用 RichFlatMapFunction，目的使用 open 和 close 这种生命周期函数
        // 此处也可使用 process function 来代替 flatMap，process function 自带 open 和 close 这种生命周期函数
        return mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取连接
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                // 关闭连接
                HBaseUtil.closeConnection(connection);
            }

            @Override
            public void flatMap(String value, Collector<TableProcessDim> out) throws Exception {
                // 使用读取的配置表数据  到HBase中创建与之对应的表格
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    // FlinkCDC 抓取的 mysql 配置维表的内容，数据格式为 JSON
                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        // 当配置表发送一个 D 类型的数据  对应 HBase 需要删除一张维度表
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(dim);
                    } else {
                        // 当更新一条表数据时，先删掉老表，在创建新表
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    out.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                String[] split = sinkFamily.split(",");
                try {
                    HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), split);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void deleteTable(TableProcessDim dim) {
                try {
                    HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 核心业务逻辑
     * 对 MaxWell 读取 db 数据库的原始数据进行数据清洗
     * @param stream
     * @return
     */
    public SingleOutputStreamOperator<JSONObject> transfer(DataStreamSource<String> stream) {

        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> out) throws Exception {
                try {
                    // MaxWell 抓取的实时数据的 json 格式
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database)
                            && !"bootstrap-start".equals(type)
                            && !"bootstrap-complete".equals(type)
                            && data != null
                            && data.size() != 0) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).returns(JSONObject.class);
    }
}
