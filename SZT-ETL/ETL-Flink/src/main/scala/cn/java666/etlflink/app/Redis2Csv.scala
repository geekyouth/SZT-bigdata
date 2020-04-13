package cn.java666.etlflink.app

import cn.java666.etlflink.sink.MyCsvSinkFun
import cn.java666.etlflink.source.MyRedisSourceFun
import org.apache.flink.streaming.api.scala._

/**
 * @author Geek
 * @date 2020-04-14 04:35:36
 *
 * redis szt:pageJson 抽取元数据到 csv 
 * 
 */
object Redis2Csv {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val s = env.addSource(new MyRedisSourceFun)
		
		s.addSink(new MyCsvSinkFun)
		
		env.execute("Redis2Csv")
	}
}
