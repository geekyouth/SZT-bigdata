package cn.java666.sztflink.realtime.sink

import java.sql.{Connection, DriverManager, Statement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @author Geek
 * @date 2020-05-24 23:28:12
 *
 * 自定义flink sink 到 clickhouse 
 * 
 */
case class MyClickhouseSinkFun(host: String = "localhost", port: Int = 8123, user: String = "default", passwd: String = "") extends RichSinkFunction[String] {
	var conn: Connection = _
	var stat: Statement = _
	
	override def open(parameters: Configuration): Unit = {
		conn = DriverManager.getConnection("jdbc:clickhouse://" + host + ":" + port, user, passwd)
		stat = conn.createStatement()
	}
	
	override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
		//val set = stat.executeQuery("select * from system.clusters").asInstanceOf[ClickHouseResultSet]
		stat.execute(s"INSERT into szt.szt_data values ('${value}')")
		println(value)
	}
	
	override def close(): Unit = {
		conn.close()
	}
}
/*

CREATE DATABASE IF NOT EXISTS szt;
CREATE TABLE szt.szt_data (string_value String) ENGINE = Log();

SELECT * FROM szt.szt_data;

*/