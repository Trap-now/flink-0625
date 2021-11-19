package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink01_SQL_GroupWindow_Tumbling {
    public static void main(String[] args) {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor-sql.txt',"
                + "'format' = 'csv'"
                + ")");

        //TODO 开启一个基于事件时间的滚动窗口
        //TODO 当读取有界数据时，使用处理时间的话会导致窗口无法关闭，因为没有数据输出
        tableEnv.executeSql( "SELECT id, " +
                "  TUMBLE_START(t, INTERVAL '3' second) as wStart,  " +
                "  TUMBLE_END(t, INTERVAL '3' second) as wEnd,  " +
                "  SUM(vc) sum_vc " +
                "FROM sensor " +
                "GROUP BY TUMBLE(t, INTERVAL '3' second), id"
        ).print();

    }
}
