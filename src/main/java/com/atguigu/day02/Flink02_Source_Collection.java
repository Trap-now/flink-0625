package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.从集合中获取数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        DataStreamSource<Integer> streamSource = env.fromCollection(list);

        //多并行度方法，核心是实现了ParallelSourceFunction这个接口
//        env.fromParallelCollection()

        streamSource.print();

        env.execute();
    }
}
