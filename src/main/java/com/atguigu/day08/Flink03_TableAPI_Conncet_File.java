package com.atguigu.day08;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

public class Flink03_TableAPI_Conncet_File {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.连接外部文件系统
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv
                .connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将映射到文件系统的表转为Table对象
        Table table = tableEnv.from("sensor");

        Table resultTable = table
                .groupBy($("id"))
                .select($("id"), $("vc").sum());

       //sql写法
//        Table resultTable = tableEnv.sqlQuery("select id,sum(vc) from sensor group by id");

        //将表转为流
        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(resultTable, Row.class);

        result.print();

        env.execute();


    }
}
