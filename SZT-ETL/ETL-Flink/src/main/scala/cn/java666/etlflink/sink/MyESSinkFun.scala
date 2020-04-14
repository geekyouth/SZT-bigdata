package cn.java666.etlflink.sink

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType

/**
 * @author Geek
 * @date 2020-04-14 18:24:33
 *
 * flink sink ES7 ，这里有很多坑！！！
 * 
 * 跑完后查询 es 相关索引：1266039 数据数量完全正确！！！
 */
case class MyESSinkFun(index: String) extends ElasticsearchSinkFunction[String] {
	override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
		// 如果是序列化样例类，或者序列化 java/scala 对象，优先 gson
		// 反序列化优先 fastjson，因为 fastjson 序列化能力不如 gson！！！
		// 不满足 json 语法规则的纯字符串无法转换为合格的 json ，无法写入 es
		
		val json = JSON.parseObject(element)
		val indexReq = new IndexRequest(index).source(json, XContentType.JSON)
		indexer.add(indexReq)
		println(s"es save element=${element} | json=${json}")
	}
}
