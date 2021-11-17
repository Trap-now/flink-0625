package com.atguigu.day08;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class Flink04_TableAPI_Conncet_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3.连接Kafka
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv
                .connect(new Kafka()
                        .topic("sensor")
                        .startFromLatest()
                        .version("universal")
                        .property(ConsumerConfig.GROUP_ID_CONFIG, "0625")
                        .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                )
                .withFormat(new Json())
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
