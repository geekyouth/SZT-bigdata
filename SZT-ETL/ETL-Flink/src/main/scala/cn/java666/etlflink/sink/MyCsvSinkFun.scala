package cn.java666.etlflink.sink

import java.util.StringJoiner

import cn.hutool.core.io.FileUtil
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.functions.sink.SinkFunction

/**
 * @author Geek
 * @date 2020-04-14 04:27:47
 *
 * 自定义 flink sink csv
 * 
 * 按天保存文件块
 */

case class MyCsvSinkFun(day: String) extends SinkFunction[String] {
	val SAVE_PATH = "/tmp/szt-data/szt-data_" + day + ".csv"
	
	override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
		// 11 个字段
		val json = JSON.parseObject(value)
		
		val deal_date = json.getString("deal_date")
		val close_date = json.getString("close_date")
		val card_no = json.getString("card_no")
		val deal_value = json.getString("deal_value")
		val deal_type = json.getString("deal_type")
		val company_name = json.getString("company_name")
		val car_no = json.getString("car_no")
		val station = json.getString("station")
		val conn_mark = json.getString("conn_mark")
		val deal_money = json.getString("deal_money")
		val equ_no = json.getString("equ_no")
		
		//val csv = (deal_date, close_date, card_no, deal_value, deal_type, company_name, car_no, station, conn_mark, deal_money, equ_no)
		val csv = new StringJoiner(",")
			.add(deal_date)
			.add(close_date)
			.add(card_no)
			.add(deal_value)
			.add(deal_type)
			.add(company_name)
			.add(car_no)
			.add(station)
			.add(conn_mark)
			.add(deal_money)
			.add(equ_no)
			.toString
		
		FileUtil.appendUtf8String(csv + "\n", SAVE_PATH)
		
		val i = FileUtil.readUtf8Lines(SAVE_PATH).size()
		// 核对 ES 数据库，记录完全一致！！！
		println(i) // szt-data_2018-09-01.csv 合计 1229180 条
	}
}
