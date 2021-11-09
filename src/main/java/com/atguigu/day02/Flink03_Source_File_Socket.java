package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink03_Source_File_Socket {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.从文件中获取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt").setParallelism(2);

        //TODO 从端口中获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        streamSource.print();


        env.execute();
    }
}
