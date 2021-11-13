package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink09_Output {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        SingleOutputStreamOperator<WaterSensor> result = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy("id")
                //采集监控传感器水位值，将水位值高于5cm的值输出到side output
                .process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (value.getVc() > 5) {
                            //水位高于5cm，则将数据发到侧输出流
                            ctx.output(new OutputTag<WaterSensor>("output") {}, value);
                        }
                        out.collect(value);
                    }
                });

        result.print("主流");

        //获取侧输出流
        DataStream<WaterSensor> sideOutput = result.getSideOutput(new OutputTag<WaterSensor>("output") {
        });

        sideOutput.print("水位高于5cm");
        env.execute();

    }
}
