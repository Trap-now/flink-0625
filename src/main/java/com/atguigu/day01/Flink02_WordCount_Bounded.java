package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.awt.peer.WindowPeer;

public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1
        env.setParallelism(1);

        //2.获取有界数据（文件中的数据）
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        //3.将每一行数据按照空格切分切出每一个单词
        SingleOutputStreamOperator<String> wordDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按照空格切分返回一个数组
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.将单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map( value -> Tuple2.of(value, 1)).returns(Types.TUPLE(Types.STRING,Types.INT));

        //5.将相同单词的数据聚和到一块
//        wordToOneDStream.keyBy(0)
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //6.对value相加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        //执行任务
        env.execute();

    }
}
