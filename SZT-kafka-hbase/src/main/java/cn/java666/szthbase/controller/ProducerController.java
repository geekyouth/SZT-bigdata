package cn.java666.szthbase.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/kafka")
@Tag(name = "Kafka 生产者", description = "Kafka 消息生产 API")
public class ProducerController {
    @Value("${kafka.producer.topic}")
    private String topicProduce;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(value = "/producer")
    @Operation(summary = "发送消息", description = "向 Kafka 发送消息")
    public void produce(@RequestBody String msg) {
        for (int i = 1; i <= 5; i++) {
            kafkaTemplate.send(topicProduce, i + msg);
        }
    }
}
