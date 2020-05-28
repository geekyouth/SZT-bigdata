package cn.java666.sztflink.realtime

import java.util.Properties

import cn.java666.sztflink.realtime.sink.MyClickhouseSinkFun
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.junit._

/**
 * @author Geek
 * @date 2020-05-24 18:14:27
 *
 * flink 读取 kafka 存到 ch【自定义】
 */
case class Kafka2MyCH() {
	
	@Test
	def test1() {
		
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val kafka_prop = new Properties
		kafka_prop.setProperty("bootstrap.servers", "cdh231:9092")
		kafka_prop.setProperty("group.id", "consumer-group-flink")
		
		env.addSource[String](
			new FlinkKafkaConsumer011("topic-flink-szt", new SimpleStringSchema, kafka_prop)
			.setStartFromEarliest()
		)
			.name("kafka-source")
			.map(x => {
				//Thread.sleep(1000)
				x
			})
			.addSink(new MyClickhouseSinkFun("cdh231"))
			.name("ch-sink")
		
		env.execute("Kafka2CH")
	}
}

/*
<!-- https://mvnrepository.com/artifact/com.github.housepower/clickhouse-native-jdbc 9000 不成功 -->
<dependency>
    <groupId>com.github.housepower</groupId>
    <artifactId>clickhouse-native-jdbc</artifactId>
    <version>2.1-stable</version>
</dependency>

<!-- 8123 正常 -->
<dependency>
    <groupId>ru.yandex.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.2.4</version>
</dependency>
 */