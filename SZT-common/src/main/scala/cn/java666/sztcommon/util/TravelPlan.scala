package cn.java666.sztcommon.util

import java.util.UUID

import cn.hutool.core.io.FileUtil
import cn.hutool.json.{JSONArray, JSONObject, JSONUtil}

/**
 * @author Geek
 * @date 2020-04-26 20:15:11
 *
 * 解析抓取后的线路规划，json 转为 csv
 */
case class TravelPlan() {
	val uuid: UUID = UUID.randomUUID
	
	val path = """D:\tmp\tralel-plans\MiniTime.jsons"""
	
	import org.junit._
	@Test
	def test1(): Unit = {
		val list = FileUtil.readUtf8Lines(path)
		for (i <- 0 until list.size()) {
			val str = list.get(i)
			val jsonStr = JSONUtil.parseObj(str)
			val start_sid = jsonStr.getStr("qidiancode")
			val end_sid = jsonStr.getStr("zhondiancode")
			val switch_counts = jsonStr.getInt("times")
			
			val lineArr = jsonStr.getJSONArray("lineList")
			val size = lineArr.size() // size = switch_counts + 2
			//子行程
			val tralev_all = new JSONArray(size - 1)
			for (j <- 0 until (size - 1)) { //最后一组是空的
				val lineObj = lineArr.get(j).asInstanceOf[JSONObject]
				val travel_time = lineObj.get("travelTime").toString
				val switch_time = lineObj.getStr("transferTime")
				val next_sid = lineObj.getStr("code")
				
				val travelObj = new JSONObject
				travelObj.put("travel_time", travel_time)
				travelObj.put("switch_time", switch_time)
				travelObj.put("next_sid", next_sid)
				tralev_all.add(j, travelObj)
			}
			
			val line = s"$start_sid\t$end_sid\t$switch_counts\t$tralev_all"
			println(line)
			FileUtil.appendUtf8String(line + "\n", "/tmp/" + uuid + "-travel_plan.csv")
		}
	}
	
	@Test
	def test2() {
		for (i <- 6 to 23) {
			for (j <- 0 to 59) {
				println(i + "," + j)
			}
		}
	}
}
