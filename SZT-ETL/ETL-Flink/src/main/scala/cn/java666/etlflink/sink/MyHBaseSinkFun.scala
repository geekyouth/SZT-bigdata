package cn.java666.etlflink.sink

import java.io.IOException
import cn.hutool.json.JSONUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, NamespaceNotFoundException, TableName}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * @author Geek
 * @date 2020-05-01 12:00:14
 *
 * 自定义 flink sink hbase 
 * @param tbNameStr 表名
 * @param ns        命名空间
 * @param versions  保留版本数
 */
case class MyHBaseSinkFun(tbNameStr: String, ns: String = "default", versions: Int = 4) extends RichSinkFunction[String] {
	private val log = LoggerFactory.getLogger(this.getClass)
	
	private val zk = "cdh231,cdh232,cdh233"
	//private val port = "2181"
	private val cf = "info"
	
	var conn: Connection = _
	var table: Table = _
	
	override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
		insert(value)
		log.warn("写入 hbase 成功 [{}]", value)
	}
	
	override def open(parameters: Configuration): Unit = {
		val config = HBaseConfiguration.create
		config.set("hbase.zookeeper.quorum", zk)
		conn = ConnectionFactory.createConnection(config)
		val admin = conn.getAdmin
		
		try {
			admin.getNamespaceDescriptor(ns)
		} catch {
			case e: NamespaceNotFoundException => {
				println(s"hbase [$ns] 命名空间不存在，将会新建")
				admin.createNamespace(NamespaceDescriptor.create(ns).build())
			}
		} finally {
			//println()
		}
		
		val nsTbName = TableName.valueOf(ns + ":" + tbNameStr)
		if (!admin.tableExists(nsTbName)) {
			admin.createTable(
				new HTableDescriptor(nsTbName)
					.addFamily(new HColumnDescriptor(cf).setMaxVersions(versions))
			)
		}
		table = conn.getTable(nsTbName)
	}
	
	override def close(): Unit = {
		//table.close()
		conn.close()
	}
	
	/** 插入新数据 */
	def insert(jsonStr: String): Unit = {
		try {
			val jsonObj = JSONUtil.parseObj(jsonStr)
			val card_no_re = StringUtils.reverse(jsonObj.getStr("card_no"))
			
			val keys = jsonObj.keySet().toList
			val size = keys.size()
			
			for (i <- 0 until size) {
				val key = keys.get(i)
				val value = jsonObj.getStr(key)
				putCell(card_no_re, cf, key, value)
			}
		} catch {
			case e: IOException => e.printStackTrace()
		}
	}
	
	def putCell(rowKey: String, family: String, column: String, value: String): Unit = {
		val put = new Put(Bytes.toBytes(rowKey))
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
		table.put(put)
	}
}
