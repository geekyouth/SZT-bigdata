package cn.java666.etlflink.app;

import cn.java666.etlflink.source.MyRedisSourceFun;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class Redis2Kafka {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var prop = new Properties();
        prop.load(ClassLoader.getSystemResourceAsStream("kafka.properties"));

        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(prop.getProperty("kafka.broker-list"))
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(prop.getProperty("kafka.producer.topic"))
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        env.addSource(new MyRedisSourceFun())
            .sinkTo(sink);

        env.execute("Redis2Kafka");
    }
}
