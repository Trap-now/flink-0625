package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink04_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<UserBehavior> userBehaviorDStream = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //4.过滤出Pv的数据
        SingleOutputStreamOperator<Long> pvToOneDStream = userBehaviorDStream.flatMap(new FlatMapFunction<UserBehavior, Long>() {
            @Override
            public void flatMap(UserBehavior value, Collector<Long> out) throws Exception {
                if ("pv".equals(value.getBehavior())) {
                    out.collect(value.getUserId());
                }
            }
        });

        //5.直接过滤重复的数据求出Uv
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = pvToOneDStream.process(new ProcessFunction<Long, Tuple2<String, Integer>>() {
            HashSet<Long> uids = new HashSet<>();

            @Override
            public void processElement(Long value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                uids.add(value);
                out.collect(Tuple2.of("uv", uids.size()));
            }
        });


        result.print();


        env.execute();
    }
}
