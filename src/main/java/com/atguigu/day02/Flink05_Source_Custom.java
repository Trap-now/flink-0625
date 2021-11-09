package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.Random;

public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2.自定义Source
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource()).setParallelism(2);
        streamSource.print();
        env.execute();
    }

    //如果想要设置多并行度，则需要实现ParallelSourceFunction这个接口
//    public static class MySource implements ParallelSourceFunction<WaterSensor> {
    public static class MySource implements SourceFunction<WaterSensor> {
        private  Random random = new Random();
        private Boolean isRunning = true;

        @Override

        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            while (isRunning) {
                Thread.sleep(200);
                ctx.collect(new WaterSensor("sensor"+random.nextInt(100), System.currentTimeMillis(), random.nextInt(1000)));
            }

        }

        /**
         * 取消数据生成和发送
         * cancel()方法是由系统内部自己调的
         */
        @Override
        public void cancel() {

            isRunning = false;
        }
    }
}
