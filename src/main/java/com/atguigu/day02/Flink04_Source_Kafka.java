package com.atguigu.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.从kafka获取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "0625");

        DataStreamSource<String> streamSource = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties)).setParallelism(4);

        streamSource.print("kafka:");

        env.execute();
    }
}
