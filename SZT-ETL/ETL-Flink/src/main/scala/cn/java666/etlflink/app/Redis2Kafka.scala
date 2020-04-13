package cn.java666.etlflink.app

import java.util.Properties

import cn.java666.etlflink.source.MyRedisSourceFun
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @author Geek
 * @date 2020-04-14 04:35:36
 *
 * redis szt:pageJson 抽取元数据到 kafka
 * 
 */
object Redis2Kafka {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val prop = new Properties
		prop.load(ClassLoader.getSystemResourceAsStream("kafka.properties"))
		
		val s = env.addSource[String](new MyRedisSourceFun)
		
		s.addSink(
			new FlinkKafkaProducer011(
				prop.getProperty("kafka.broker-list")
				, prop.getProperty("kafka.producer.topic")
				, new SimpleStringSchema()
			)
		)
		
		env.execute("MyRedisSource")
	}
}
