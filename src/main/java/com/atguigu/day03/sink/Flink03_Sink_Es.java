package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

public class Flink03_Sink_Es {
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
        SingleOutputStreamOperator<String> jsonStrDStream = waterSensorDStream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return JSONObject.toJSONString(value);
            }
        });

        //TODO 将数据写入Es中
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        httpHosts.add(new HttpHost("hadoop103", 9200));
        httpHosts.add(new HttpHost("hadoop104", 9200));

        ElasticsearchSink.Builder<String> esBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {

                IndexRequest indexRequest = new IndexRequest("0625", "_doc", "1001");
                indexRequest.source(element, XContentType.JSON);

                indexer.add(indexRequest);
            }
        });

        //数据来一条写一条
        esBuilder.setBulkFlushMaxActions(1);

        jsonStrDStream.addSink(esBuilder.build());


        env.execute();

    }
}
