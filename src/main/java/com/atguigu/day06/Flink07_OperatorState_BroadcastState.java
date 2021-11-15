package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_OperatorState_BroadcastState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取两个流
        DataStreamSource<String> hadoopDStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> localhostDStream = env.socketTextStream("localhost", 9999);

        //3.定义广播状态
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>("state",String.class,String.class);

        //4.广播状态（广播流）
        BroadcastStream<String> broadcast = localhostDStream.broadcast(mapStateDescriptor);

        //5.连接两条流
        BroadcastConnectedStream<String, String> connect = hadoopDStream.connect(broadcast);

        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                //提取状态中的数据
                String aSwitch = broadcastState.get("switch");

                if ("1".equals(aSwitch)) {
                    out.collect("写入Hbase");
                } else if ("2".equals(aSwitch)) {
                    out.collect("写入Kafka");
                } else {
                    out.collect("写入文件");
                }

            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                //1.提取状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);


                //2.向状态中存入数据
                broadcastState.put("switch", value);

            }
        }).print();

        env.execute();
    }
}
