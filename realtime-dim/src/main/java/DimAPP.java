import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.taitan.flink.realtime.common.base.BaseAPP;
import org.taitan.flink.realtime.common.constant.Constant;
import org.taitan.flink.realtime.common.util.FlinkSourceUtil;

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
//        stream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String value) throws Exception {
//                boolean flat = false;
//                try {
//                    JSONObject jsonObject = JSONObject.parseObject(value);
//                    String database = jsonObject.getString("database");
//                    String type = jsonObject.getString("type");
//                    JSONObject data = jsonObject.getJSONObject("data");
//                    if ("gmall".equals(database) &&
//                            !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)
//                            && data != null && data.size() != 0){
//                        flat = true;
//                    }
//                }catch (Exception e){
//                    e.printStackTrace();
//                }
//                return flat;
//            }
//        }).map(JSONObject::parseObject);

        SingleOutputStreamOperator<JSONObject> jsonObjStream = transfer(stream);

        // 2. 使用flinkCDC读取监控配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        // 使用 FlinkCDC 读取数据时，并行度设置为 1，否则会出问题
        DataStreamSource<String> mysqlSource = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);
//        mysqlSource.print();

    }

    /**
     *
     * @param stream
     * @return
     */
    public SingleOutputStreamOperator<JSONObject> transfer(DataStreamSource<String> stream) {

        return stream.flatMap((FlatMapFunction<String, JSONObject>) (value, out) -> {
            try {
                // MaxWell 抓取的实时数据的 json 格式
                JSONObject jsonObject = JSONObject.parseObject(value);
                String database = jsonObject.getString("database");
                String type = jsonObject.getString("type");
                JSONObject data = jsonObject.getJSONObject("data");
                if ("gmall".equals(database)
                        && !"bootstrap-start".equals(type)
                        && !"bootstrap-complete".equals(type)
                        && data != null && data.size() != 0) {
                    out.collect(jsonObject);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).returns(JSONObject.class);
    }
}
