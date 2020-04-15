package cn.java666.etlflink.app

import cn.java666.etlflink.sink.MyCsvSinkFun
import cn.java666.etlflink.source.MyRedisSourceFun
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.scala._

/**
 * @author Geek
 * @date 2020-04-14 04:35:36
 *
 * redis szt:pageJson 抽取源数据到 csv 
 * 
 * csv 按天保存
 */
object Redis2Csv {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		val ymd = "2018-09-01"
		
		// 添加过滤器：source | transform | sink 三个地方都可以自由定制过滤规则。
		env.addSource(new MyRedisSourceFun)
			.filter(x => {
				val json = JSON.parseObject(x)
				val deal_date = json.getString("deal_date")
				deal_date.startsWith(ymd)
			})
			.addSink(new MyCsvSinkFun(ymd))
		
		env.execute("Redis2Csv")
	}
}

/*
"deal_date": "2018-08-31 22:14:50",
"close_date": "2018-09-01 00:00:00",
"card_no": "CBEHFCFCG",
"deal_value": "0",
"deal_type": "地铁入站",
"company_name": "地铁五号线",
"car_no": "IGT-105",
"station": "布吉",
"conn_mark": "0",
"deal_money": "0",
"equ_no": "263032105"
*/