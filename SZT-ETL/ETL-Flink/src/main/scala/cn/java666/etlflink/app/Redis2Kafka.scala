package cn.java666.etlflink.app

import java.util.Properties

import cn.java666.etlflink.source.MyRedisSourceFun
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

import scala.util.Random

/**
 * @author Geek
 * @date 2020-04-14 04:35:36
 *
 * redis szt:pageJson 抽取源数据到 kafka
 *
 */
object Redis2Kafka {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val prop = new Properties
		prop.load(ClassLoader.getSystemResourceAsStream("kafka.properties"))
		
		val s = env.addSource[String](new MyRedisSourceFun)
			.map(x => {
				// TODO 假装休息一会，如果客户觉得速度太慢，可以加钱优化！！！但是这里我们真的需要休息，模拟流式数据连续的注入
				//Thread.sleep(Random.nextInt(1000))
				x
			})
		
		s.addSink(
			new FlinkKafkaProducer011(
				prop.getProperty("kafka.broker-list")
				, prop.getProperty("kafka.producer.topic")
				, new SimpleStringSchema()
			)
		)
		
		env.execute("Redis2Kafka")
	}
}
