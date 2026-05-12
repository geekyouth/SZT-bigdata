package cn.java666.sztflink.realtime;

import cn.java666.sztflink.realtime.sink.MyClickhouseSinkFun;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class Kafka2MyCH {

    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("cdh231:9092")
            .setTopics("topic-flink-szt")
            .setGroupId("consumer-group-flink")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
            .addSink(new MyClickhouseSinkFun("cdh231"))
            .name("ch-sink");

        env.execute("Kafka2CH");
    }
}
