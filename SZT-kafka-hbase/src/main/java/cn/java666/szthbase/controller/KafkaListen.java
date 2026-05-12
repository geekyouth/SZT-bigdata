package cn.java666.szthbase.controller;

import cn.java666.sztcommon.pojo.SztDataBean;
import cn.java666.szthbase.service.SztDataService;
import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaListen {
    @Value("${kafka.consumer.topic}")
    private String topic;

    private final SztDataService service;

    public KafkaListen(SztDataService service) {
        this.service = service;
    }

    @KafkaListener(topics = {"${kafka.consumer.topic}"})
    public void listen(ConsumerRecord<?, ?> record) {
        String value = (String) record.value();
        sink2Hbase(value);
    }

    private void sink2Hbase(String jsonStr) {
        service.insert(JSONObject.parseObject(jsonStr, SztDataBean.class));
    }
}
