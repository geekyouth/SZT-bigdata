package cn.java666.sztflink.realtime

import java.util.Properties

import cn.java666.sztflink.realtime.sink.MyClickhouseSinkFun
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * @author Geek
 * @date 2020-05-24 18:14:27
 *
 * flink 读取 kafka 存到 ch【自定义】
 */
case class Kafka2MyCH() {
	
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val kafka_prop = new Properties
		kafka_prop.setProperty("bootstrap.servers", "cdh231:9092")
		kafka_prop.setProperty("group.id", "consumer-group-flink")
		
		env.addSource[String](new FlinkKafkaConsumer011("topic-flink-szt", new SimpleStringSchema, kafka_prop))
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
