package org.taitan.flink.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.taitan.flink.realtime.common.bean.TableProcessDim;
import org.taitan.flink.realtime.common.constant.Constant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Copyright 2020-2030 邹德翔 All Rights Reserved.
 *
 * @Author dexiang.zou
 * @Date 2025/2/15 14:25
 * @Version 1.0.0
 * <p>
 * Describe:
 */
public class HBaseUtil {

    /**
     * 获取 HBase 连接
     * @return 一个 HBase 的同步连接
     */
    public static Connection getConnection()  {
        // 使用Conf
//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        // 使用配置文件
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭 HBase 连接
     * @param connection 一个 HBase 的同步连接
     */
    public static void closeConnection(Connection connection){
        if (connection != null && !connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表格
     * @param connection 一个 HBase 的同步连接
     * @param namespace  命名空间
     * @param table      表名
     * @param families   列族名 可以是多个
     * @throws IOException  补获 admin 连接异常
     */
    public static void createTable(Connection connection, String namespace, String table, String... families) throws IOException {

        if (families == null || families.length == 0){
            System.out.println("创建HBase至少有一个列族");
            return;
        }

        // 1. 获取admin
        Admin admin = connection.getAdmin();

        // 2. 创建表格描述
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));

        for (String family : families) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder
                            .newBuilder(Bytes.toBytes(family))
                            .build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        // 3. 使用 admin 调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前表格已经存在，不需要重复创建" + namespace + ":" + table);
        }

        // 4. 关闭admin
        admin.close();
    }


    /**
     * 删除表格
     * @param connection  一个 HBase 的同步连接
     * @param namespace   命名空间
     * @param table       表名
     * @throws IOException 补获 admin 连接异常
     */
    public static void dropTable(Connection connection,String namespace,String table) throws IOException {
        // 1.获取admin
        Admin admin = connection.getAdmin();

        // 2. 调用方法删除表格
        try {
            admin.disableTable(TableName.valueOf(namespace,table));
            admin.deleteTable(TableName.valueOf(namespace,table));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭admin
        admin.close();
    }

    /**
     * 写数据到HBase
     * @param connection 一个同步连接
     * @param namespace  命名空间
     * @param tableName  表名
     * @param rowKey    主键
     * @param family    列族名
     * @param data     列名和列值  jsonObj对象
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String family, JSONObject data) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建写入对象
        Put put = new Put(Bytes.toBytes(rowKey));

        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null){
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(columnValue));
            }
        }

        // 3. 调用方法写出数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4. 关闭table
        table.close();
    }

    /**
     * 删除一整行数据
     * @param connection  一个同步连接
     * @param namespace   命名空间名称
     * @param tableName   表格
     * @param rowKey     主键
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 3. 调用方法删除数据
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4. 关闭table
        table.close();
    }
}
