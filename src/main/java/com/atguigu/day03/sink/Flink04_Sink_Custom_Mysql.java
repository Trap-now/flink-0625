package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class Flink04_Sink_Custom_Mysql {
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

        //TODO 自定义Sink将数据写入Mysql
        waterSensorDStream.addSink(new MySink());

        env.execute();

    }

    public static class MySink extends RichSinkFunction<WaterSensor> {

        private Connection connection;
        private PreparedStatement pstm;

        @Override
        public void open(Configuration parameters) throws Exception {
            //1.创建连接
             connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "000000");

            //2.创建语句预执行者
             pstm = connection.prepareStatement("insert into sensor values (?,?,?)");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            //3.给占位符赋值
            pstm.setString(1, value.getId());
            pstm.setLong(2, value.getTs());
            pstm.setInt(3, value.getVc());

            pstm.execute();


        }

        @Override
        public void close() throws Exception {
            //关闭链接
            pstm.close();
            connection.close();
        }
    }
}
