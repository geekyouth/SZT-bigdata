package cn.java666.sztcommon.util

import java.util.UUID

import cn.hutool.core.io.FileUtil
import cn.hutool.core.util.StrUtil
import cn.hutool.http.HttpUtil
import cn.hutool.json.{JSONObject, JSONUtil}
import org.junit.Test

import scala.util.control.Breaks

/**
 * @author Geek
 * @date 2020-04-25 23:51:08
 *
 * 解析深圳地铁站名，遍历所有可能的线路规划方案。合计 45932 种排列算法。
 * 
 */
case class SZmetro() {
	val uUID = UUID.randomUUID
	
	/** 解析 szmc.net-metro.json 到 szmc.net-metro.csv 
	 * 1,地铁1号线,1,罗湖,0101 
	 */
	@Test
	def json2CSV() {
		val s = FileUtil.readUtf8String(ClassLoader.getSystemResource("szmc.net-metro.json").getPath)
		val obj = JSONUtil.parseObj(s)
		val lineArray = obj.getJSONArray("l")
		
		val lineSize = lineArray.size()
		for (i <- 0 until lineSize) {
			val lineObject = lineArray.get(i).asInstanceOf[JSONObject]
			val line_name_ws_alias = lineObject.getStr("kn") // "kn": "地铁1号线(罗宝线)", 
			val line_name = StrUtil.subBefore(line_name_ws_alias, "(", false)
			
			val stationArray = lineObject.getJSONArray("st")
			val stationSize = stationArray.size()
			
			for (i <- 0 until stationSize) {
				Breaks.breakable {
					val stationObject = stationArray.get(i).asInstanceOf[JSONObject]
					val sName = stationObject.getStr("n") // 站名
					val sID = stationObject.getStr("poiid") // 线号站号 0902 | "poiid": "BV10246013",
					// 跳过 幽灵车站，大剧院 BV10246013 
					if (sID.contains("BV10246013")) {
						Breaks.break()
					}
					val line_no = sID.substring(0, 2).toInt
					val station_no = sID.substring(2, 4).toInt
					val value = s"$line_no,$line_name,$station_no,$sName,$sID"
					println(value)
					FileUtil.appendUtf8String(value + "\n", "/tmp/" + uUID + "/szmc.net-metro.csv")
				}
			}
			//println("")
		}
	}
	
	/** 2-站站组合结果用来查询线路规划 */
	def searchPlan(sid1: String, sid2: String): Unit = {
		val res = HttpUtil.createPost("https://www.szmc.net/algorithm/Ticketing/MinTimeJson.do")
			.header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
			.body(s"departureStation=$sid1&arriveStation=$sid2&ridingType=0")
			.execute()
		
		if (res.getStatus != 200) {
			searchPlan(sid1, sid2)
		} else {
			val resObj = JSONUtil.parseObj(res.body())
			
			//3-所有线路规划写入 csv 表 | 文件容量 74MB
			FileUtil.appendUtf8String(resObj.toString + "\n", s"/tmp/$uUID/MiniTime.jsons")
			println(resObj)
		}
	}
	
	/** 抓取过程大概耗时 50分钟，中途不要操作写入文件 */
	@Test
	def loopPlan() {
		//1-排列组合，站站组合方式 | szmc.net-metro.csv
		val list = FileUtil.readUtf8Lines(ClassLoader.getSystemResource("szmc.net-metro.csv").getPath)
		val size = list.size()
		
		//出发站任意
		for (item1 <- 1 to size) { // 遍历站点
			val s1 = list.get(item1 - 1)
			val arr1 = s1.split(",")
			val sid1 = arr1(4) //逻辑出发点
			val sName1 = arr1(3)
			
			//到达站任意，但不能在原地
			for (item2 <- 1 to size) { // 遍历站点
				if (item2 != item1) {
					val s2 = list.get(item2 - 1)
					val arr2 = s2.split(",")
					val sid2 = arr2(4) //逻辑到达点
					val sName2 = arr2(3)
					println(s"$sid1 $sName1 --> $sid2 $sName2 | ") //1101 福田 --> 1102 车公庙 | 
					
					// 路径规划，跳过同名站点
					if (!sName2.equals(sName1)) {
						searchPlan(sid1, sid2)
					}
				}
			}
		}
	}
}
