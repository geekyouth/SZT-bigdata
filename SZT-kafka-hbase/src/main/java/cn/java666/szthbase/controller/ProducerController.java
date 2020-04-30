package cn.java666.szthbase.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author Geek
 * @date 2020-04-30 22:16:30
 */

@RestController
@RequestMapping(value = "/kafka")
public class ProducerController {
	@Value("${kafka.producer.topic}")
	private String topic_produce;
	
	@Resource
	private KafkaTemplate kafkaTemplate;
	
	@PostMapping(value = "/producer")
	public void consume(@RequestBody String msg) {
		for (int i = 1; i <= 5; i++) {
			kafkaTemplate.send(topic_produce, i + msg);
		}
	}
}
