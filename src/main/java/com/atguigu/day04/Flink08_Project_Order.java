package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink08_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件分别获取数据
        DataStreamSource<String> orderLogDStream = env.readTextFile("input/OrderLog.csv");

        DataStreamSource<String> receiptLogDStream = env.readTextFile("input/ReceiptLog.csv");

        //3.分别将两条流的数据转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderDStream = orderLogDStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        });

        SingleOutputStreamOperator<TxEvent> txDStream = receiptLogDStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });

        //4.将两条流连接到一块
        ConnectedStreams<OrderEvent, TxEvent> connect = orderDStream.connect(txDStream);

        //5.对相同TxId的数据聚和到一块
        ConnectedStreams<OrderEvent, TxEvent> keyedStream = connect.keyBy("txId", "txId");

        //6.对两条流的数据进行实时对账
        SingleOutputStreamOperator<String> result = keyedStream.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {

            HashMap<String, OrderEvent> orderMap = new HashMap<>();

            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                //1.查询对方缓存中是否有关联上的数据
                if (txMap.containsKey(value.getTxId())) {
                    //证明有能关联上的数据
                    out.collect("订单：" + value.getOrderId() + "对账成功");
                    //删除对方缓存中已经关联上的数据
                    txMap.remove(value.getTxId());
                } else {
                    //没有能关联上的数据,则把自己缓存到Map集合中
                    orderMap.put(value.getTxId(), value);
                }

            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                //1.查询对方缓存中是否有关联上的数据
                if (orderMap.containsKey(value.getTxId())) {
                    //证明有能关联上的数据
                    out.collect("订单：" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                    //删除对方缓存中已经关联上的数据
                    orderMap.remove(value.getTxId());
                } else {
                    //没有能关联上的数据,则把自己缓存到Map集合中
                    txMap.put(value.getTxId(), value);
                }
            }
        });

        result.print();

        env.execute();


    }
}
