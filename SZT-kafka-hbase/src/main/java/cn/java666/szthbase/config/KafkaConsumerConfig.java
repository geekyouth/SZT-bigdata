package cn.java666.szthbase.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Geek
 * @date 2020-04-30 22:10:05
 *
 * kafka消费者配置
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {
	
	@Value("${kafka.consumer.servers}")
	private String servers;
	@Value("${kafka.consumer.enable.auto.commit}")
	private boolean enableAutoCommit;
	@Value("${kafka.consumer.session.timeout}")
	private String sessionTimeout;
	@Value("${kafka.consumer.auto.commit.interval}")
	private String autoCommitInterval;
	@Value("${kafka.consumer.group.id}")
	private String groupId;
	@Value("${kafka.consumer.auto.offset.reset}")
	private String autoOffsetReset;
	@Value("${kafka.consumer.concurrency}")
	private int concurrency;
	@Value("${kafka.consumer.max.poll.records}")
	private int poll_records;
	
	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(concurrency);
		factory.getContainerProperties().setPollTimeout(1500);
		return factory;
	}
	
	public ConsumerFactory<String, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}
	
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> prop = new HashMap<>();
		prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
		prop.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
		prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
		prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);// 由 group 自动维护
		prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, poll_records);
		return prop;
	}
	
}
