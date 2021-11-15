package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class Flink12_Timer_Exec {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        SingleOutputStreamOperator<String> result = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                })
                .keyBy("id")
                //监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
                .process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    //声明一个变量用来保存上一次的水位
                    private Integer lastVc = Integer.MIN_VALUE;
                    //声明一个变量用来保存定时器的时间
                    private Long timer = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        //判断水位是否上升
                        if (value.getVc() > lastVc) {
                            //水位上升
                            //注册定时器
                            if (timer == Long.MIN_VALUE) {
                                //没有注册过定时器
                                timer = System.currentTimeMillis() + 5000;
                                System.out.println("注册定时器：" +ctx.getCurrentKey()+ timer);
                                ctx.timerService().registerProcessingTimeTimer(timer);
                            }
                        } else {
                            //如果水位没上升
                            //删除定时器
                            System.out.println("删除定时器:" + ctx.getCurrentKey()+timer);
                            ctx.timerService().deleteProcessingTimeTimer(timer);
                            //重置定时器时间
                            timer = Long.MIN_VALUE;
                        }
                        //将水位更新
                        lastVc = value.getVc();
                        out.collect(value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        //将报警信息放到侧输出流中
                        ctx.output(new OutputTag<String>("output") {
                        }, ctx.getCurrentKey()+"报警！！！水位连续5s上升");
                        //重置定时器时间
                        timer = Long.MIN_VALUE;

                    }
                });

        result.print("主流");

        result.getSideOutput(new OutputTag<String>("output") {
        }).print();

        env.execute();
    }
}
