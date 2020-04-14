package cn.java666.etlflink.source

import cn.java666.etlflink.util.RedisUtil
import com.alibaba.fastjson.JSON
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import redis.clients.jedis.Jedis
import scala.collection.JavaConversions._

/**
 * @author Geek
 * @date 2020-04-14 04:31:47
 *
 * 自定义 flink source redis
 * 根据业务需要，自由定制
 *
 */

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
					//System.err.println(" data error ------------------ x=" + x)// TODO 可选是否打印脏数据
				} else {
					// 只保留字段长度为 11 的源数据 ===> kafka: topic-flink-szt
					ctx.collect(xStr)
				}
			})
		}
	}
	
	override def cancel(): Unit = close()
	
	override def close(): Unit = client.close()
}