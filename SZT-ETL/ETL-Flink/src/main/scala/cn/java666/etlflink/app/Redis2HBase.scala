package cn.java666.etlflink.app

import org.apache.flink.streaming.api.scala._
import cn.java666.etlflink.sink.MyHBaseSinkFun
import cn.java666.etlflink.source.MyRedisSourceFun

import scala.util.Random

/**
 * @author Geek
 * @date 2020-05-01 11:13:13
 *
 * 从 redis 或者其他数据源取出 json 串，保存到 hbase 表。
 * 
 */
object Redis2HBase {
	def main(args: Array[String]): Unit = {
		
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		env.addSource(new MyRedisSourceFun)
			.map(x => {
				//println(s"读取数据=$x")
				Thread.sleep(Random.nextInt(100)) // 机器扛不住，假装休息一会
				x
			})
			.addSink(new MyHBaseSinkFun("flink2hbase", "flink", 10))
		
		env.execute("Kafka2HBase")
	}
}
