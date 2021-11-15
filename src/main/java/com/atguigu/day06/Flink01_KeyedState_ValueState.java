package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_KeyedState_ValueState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据并转为JavaBean->判断连续两个水位差值是否超过10
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy("id")
                .process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    //TODO 定一个键控状态类型用来保存上一次的水位
                    private ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //TODO 初始化状态
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //判断连续两个水位差值是否超过10
                        //TODO 使用状态
                        //1.从状态中获取上一次水位值
                        Integer lastVc = valueState.value() == null ? 0 : valueState.value();

                        //2.拿当前水位和上一次水位对比
                        if (Math.abs(value.getVc() - lastVc) > 10) {
                            out.collect("水位超过10");
                        }

                        //TODO 更新状态
                        //3.将当前水位保存到状态中
                        valueState.update(value.getVc());


                    }
                }).print();

        env.execute();
    }
}
