package cn.java666.etlflink.sink

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @author Geek
 * @date 2020-04-13 18:16:27
 *
 * redis 排序去重 ok
 *
 */
object RedisSinkPageJson {
	val SVAE_PATH = "/tmp/szt-data/szt-data-page.jsons"
	
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val s = env.readTextFile(SVAE_PATH)
			.filter(_.nonEmpty)
			.map(x => {
				JSON.parseObject(x)
			})
		
		//定义 redis 参数
		val jedis = new FlinkJedisPoolConfig.Builder().setHost("localhost").build()
		
		//存到 redis sink
		s.addSink(new RedisSink(jedis, new MyRedisSinkFun))
		
		env.execute("MyRedisSink")
	}
}

case class MyRedisSinkFun() extends RedisMapper[JSONObject] {
	override def getCommandDescription: RedisCommandDescription = {
		new RedisCommandDescription(RedisCommand.HSET, "szt:pageJson")
	}
	
	override def getKeyFromData(data: JSONObject): String = data.getIntValue("page").toString
	
	override def getValueFromData(data: JSONObject): String = data.toString
}
