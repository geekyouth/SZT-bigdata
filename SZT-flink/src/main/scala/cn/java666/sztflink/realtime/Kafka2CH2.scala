//package cn.java666.sztflink.realtime
//
//import java.util.Properties
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
//import org.junit.Test
//import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink
//import ru.ivi.opensource.flinkclickhousesink.model.{ClickhouseClusterSettings, ClickhouseSinkConsts}
//
///**
// * @author Geek
// * @date 2020-05-24 18:14:27
// *
// * flink 读取 kafka 存到 ch【第三方】
// * 
// * 暂不可用
// * 
// */
//@deprecated
//case class Kafka2CH2() {
//	
//	@Test
//	def test1() {
//		val env = StreamExecutionEnvironment.getExecutionEnvironment
//		env.setParallelism(1)
//		
//		val kafka_prop = new Properties
//		kafka_prop.setProperty("bootstrap.servers", "cdh231:9092")
//		kafka_prop.setProperty("group.id", "consumer-group-flink")
//		
//		val globalParameters = new java.util.HashMap[String, String]()
//		globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "cdh231:8123")
//		globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_USER, "default")
//		globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, "")
//		
//		//sink
//		globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, "3000")
//		globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, "/tmp")
//		globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, "1")
//		globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, "1")
//		globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, "1000")
//		
//		val env_config = env.getConfig
//		env_config.setGlobalJobParameters(ParameterTool.fromMap(globalParameters))
//		
//		val ch_prop = new Properties
//		ch_prop.setProperty(ClickhouseSinkConsts.TARGET_TABLE_NAME, "test.test2")
//		ch_prop.setProperty(ClickhouseSinkConsts.MAX_BUFFER_SIZE, "1000")
//		
//		env.addSource[String](new FlinkKafkaConsumer011("topic-flink-szt", new SimpleStringSchema, kafka_prop))
//			.name("kafka-source")
//			.map(x => {
//				//Thread.sleep(1000)
//				x
//			})
//			//.print()
//			.addSink(new ClickhouseSink(ch_prop))
//			.name("ch-sink")
//		
//		env.execute("Kafka2CH")
//	}
//}
