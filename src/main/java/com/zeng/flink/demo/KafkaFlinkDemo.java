package com.zeng.flink.demo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zeng.flink.util.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KafkaFlinkDemo {

    public static void main(String[] args) throws Exception {
        //第一步：获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //第二步：读取Kafka数据源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaUtils.BOOTSTRAPSERVERS)
                .setTopics(KafkaUtils.TOPICS)
                .setGroupId(KafkaUtils.GROUPID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //第三步：数据转换处理
//        DataStream<Tuple2<String, Integer>> newsCounts = stream.flatMap(new LineSplitter())
//                .keyBy(data ->data.f0)
//                .sum(1);
//        DataStream<Tuple2<String, Integer>> periodCounts = stream.flatMap(new LineSplitter2())
//                .keyBy(data -> data.f0)
//                .sum(1);
        //第四步：数据输出
//        newsCounts.addSink(new MySQLSink());
//        periodCounts.addSink(new MySQLSink2());
        //第五步：触发执行
        env.execute("FlinkMySQL");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            JSONObject baseJson =JSONObject.parseObject(value);
            JSONArray eventListJsonArray =  baseJson.getJSONArray("event_list");
            for (int i=0;i<eventListJsonArray.size();i++){
                JSONObject eventJsonObject=eventListJsonArray.getJSONObject(i);
                Object eventName = eventJsonObject.get("event_name");
                if (null!=eventName&&eventName.equals("click")){
                    JSONObject eventBody=eventJsonObject.getJSONObject("event_body");
                    out.collect(new Tuple2<String, Integer>(eventBody.get("news_id").toString(), 1));
                }
            }
        }
    }

    public static final class LineSplitter2 implements  FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            JSONObject baseJson =JSONObject.parseObject(value);
            JSONArray eventListJsonArray =  baseJson.getJSONArray("event_list");
            for (int i=0;i<eventListJsonArray.size();i++){
                JSONObject eventJsonObject=eventListJsonArray.getJSONObject(i);
                Object eventName = eventJsonObject.get("event_name");
                System.out.println("event_name="+eventName);
                if (null!=eventName&&eventName.equals("click")){
                    String eventTime = eventJsonObject.get("event_time").toString();
                    out.collect(new Tuple2<String, Integer>(eventTime.split("\\s+")[1], 1));
                }
            }
        }
    }
}
