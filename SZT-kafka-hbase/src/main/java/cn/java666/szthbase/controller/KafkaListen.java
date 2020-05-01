package cn.java666.szthbase.controller;

import cn.java666.sztcommon.pojo.SztDataBean;
import cn.java666.szthbase.service.SztDataService;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;

/**
 * @author Geek
 * @date 2020-04-30 22:25:00
 * 
 * kafka 监听器
 */
@Component
@Slf4j
@Controller
public class KafkaListen {
	@Value("${kafka.consumer.topic}")
	private String topic;
	
	@Resource
	private SztDataService service;
	
	/** 实时监听kafka数据 */
	@KafkaListener(topics = {"${kafka.consumer.topic}"})
	public void listen(ConsumerRecord<?, ?> record) {
		String value = (String) record.value();
		// log.warn("消费 [{}] | {}", topic, value);
		
		sink2Hbase(value);// 如果达到业务瓶颈，可以考虑采用 异步非阻塞 方式写入。其次还要考虑 hbase 写入承受能力
	}
	
	/** 疯狂写入 hbase 可能导致 hbase oom，需要加大内存 */
	private void sink2Hbase(String jsonStr) {
		service.insert(JSONObject.parseObject(jsonStr, SztDataBean.class));
	}
}
