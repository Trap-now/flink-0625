package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink08_StateBackend {
    public static void main(String[] args) throws IOException {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 设置状态后端
        env.setStateBackend(new MemoryStateBackend());

        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-ck"));

        //第二个参数代表，在写入状态时是否为增量写，如果是true的话则为增量写入
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink-ck-RD", true));

        //barrier不对齐
        env.getCheckpointConfig().enableUnalignedCheckpoints();
    }
}
