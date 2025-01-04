package com.zeng.flink.demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo1 {

    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        //2.读取文本流，ip,端口
        DataStreamSource<String> ds = env.socketTextStream("127.0.0.1", 8888);
        //3.打印结果
        ds.print();
        //4.执行flink应用
        env.execute();
    }
}
