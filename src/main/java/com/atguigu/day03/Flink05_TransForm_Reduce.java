package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_TransForm_Reduce {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

//        waterSensorDStream.keyBy(r -> r.getId());
//        waterSensorDStream.keyBy(WaterSensor::getId);
//        waterSensorDStream.keyBy("id");
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        //TODO 3.reduce
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            /**
             *
             * @param value1 聚合过后的结果
             * @param value2 当前的数据
             * @return
             * @throws Exception
             */
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                System.out.println("value1:"+value1);
//                System.out.println("value2:"+value2);
                System.out.println("recue...");
                return new WaterSensor(value1.getId(), value1.getTs(), Math.max(value1.getVc(), value2.getVc()));
            }
        });

        result.print();


        env.execute();

    }
}
