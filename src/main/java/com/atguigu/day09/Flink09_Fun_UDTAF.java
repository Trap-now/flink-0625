package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink09_Fun_UDTAF {
    public static void main(String[] args) {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口获取数据
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                });

        //3.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.将流转为表
        Table table = tableEnv.fromDataStream(waterSensorDStream);

        //不注册直接使用函数
    /*    table
                .groupBy($("id"))
                .flatAggregate(call(MyUDTAF.class, $("vc")).as("value", "rank"))
                .select($("id"),$("value"),$("rank"))
        .execute()
        .print();*/

//       先注册再使用

        //注册函数
        tableEnv.createTemporarySystemFunction("MyUDTAF", MyUDTAF.class);

              table
                .groupBy($("id"))
                .flatAggregate(call("MyUDTAF", $("vc")).as("value", "rank"))
                .select($("id"),$("value"),$("rank"))
        .execute()
        .print();






    }

    public static class MyTopAcc{
        public Integer first;
        public Integer second;
    }
    //自定义一个类实现UDAF(表聚合函数TableAggFunction)函数 根据vc求出最大的两个VC
   public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer,Integer>,MyTopAcc>{

        @Override
        public MyTopAcc createAccumulator() {
            MyTopAcc topAcc = new MyTopAcc();
            topAcc.first = Integer.MIN_VALUE;
            topAcc.second = Integer.MIN_VALUE;
            return topAcc;
        }

        public void accumulate(MyTopAcc acc,Integer value){
            //判断当前数位是否排第一
            if (value>acc.first){
                acc.second = acc.first;
                acc.first = value;
                //如果不排第一的话是否排第二
            }else if (value>acc.second){
                acc.second = value;
            }
        }

        public void emitValue(MyTopAcc acc, Collector<Tuple2<Integer,Integer>> out){
            if (acc.first!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.first,1));
            }

            if (acc.second!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.second,2));
            }
        }

    }
}
