package cn.java666.etlflink.source

import java.util.Properties

import cn.java666.etlflink.util.RedisUtil
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
 * @author Geek
 * @date 2020-04-14 00:21:46
 *
 * 自定义 flink source redis，不可用于生产，无法保证断点续传。
 * 
 * 推送元数据到 kafka 集群，只保留 11 个长度的完整元数据
 */
object MyRedisSource {
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

case class MyRedisSourceFun() extends RichSourceFunction[String] {
	var client: Jedis = _
	
	override def open(parameters: Configuration): Unit = {
		client = RedisUtil.getJedisClient
	}
	
	override def run(ctx: SourceContext[String]): Unit = {
		open(new Configuration)
		
		for (i <- 1 to 1337) {
			val v = client.hget("szt:pageJson", i + "")
			val json = JSON.parseObject(v)
			val array = json.getJSONArray("data")
			if (array.size() != 1000) {
				System.err.println(" ----- array size error ---- i=" + i) //这里没有问题
			}
			array.foreach(x => {
				val xStr = x.toString
				val data = JSON.parseObject(xStr)
				//if (data.size() != 11 && data.size() != 9) { //这里长度不统一，9|11
				if (data.size() != 11) { //这里长度不统一，9|11
					System.err.println(" data error ------------------ x=" + x)
				} else {
					// 只保留字段长度为 11 的元数据 ===> kafka: topic-flink-szt
					ctx.collect(xStr)
				}
			})
		}
	}
	
	override def cancel(): Unit = close()
	
	override def close(): Unit = client.close()
}

/* 大部分元数据长度为 11
{"deal_date":"2018-08-31 22:14:50","close_date":"2018-09-01 00:00:00","card_no":"CBEHFCFCG","deal_value":"0","deal_type":"地铁入站","company_name":"地铁五号线","car_no":"IGT-105","station":"布吉","conn_mark":"0","deal_money":"0","equ_no":"263032105"}
{"deal_date":"2018-08-31 22:13:39","close_date":"2018-09-01 00:00:00","card_no":"CBCEBDIJE","deal_value":"0","deal_type":"地铁入站","company_name":"地铁五号线","car_no":"IGT-105","station":"布吉","conn_mark":"0","deal_money":"0","equ_no":"263032105"}
{"deal_date":"2018-08-31 23:11:06","close_date":"2018-09-01 00:00:00","card_no":"FFHEDIBCC","deal_value":"700","deal_type":"地铁出站","company_name":"地铁五号线","car_no":"OGT-101","station":"长龙","conn_mark":"0","deal_money":"665","equ_no":"263031101"}
*/

/* 少部分元数据长度为 9，考虑扔掉。
parse error ------------------ x=
{"deal_date":"2018-09-01 05:28:02","close_date":"2018-09-01 00:00:00","card_no":"HHAAJAGBE","deal_value":"200","deal_type":"地铁出站","company_name":"地铁五号线","conn_mark":"0","deal_money":"0","equ_no":"263020143"}
{"deal_date":"2018-09-01 04:40:51","close_date":"2018-09-01 00:00:00","card_no":"HHAAJBCIE","deal_value":"0","deal_type":"地铁入站","company_name":"地铁三号线","conn_mark":"0","deal_money":"0","equ_no":"261009156"}

缺少关键字段："car_no": "IGT-104", "station": "布吉",

*/