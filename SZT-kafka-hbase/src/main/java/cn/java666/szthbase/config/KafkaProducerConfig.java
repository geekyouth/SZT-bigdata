package cn.java666.szthbase.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Geek
 * @date 2020-04-30 22:06:15
 * 
 * kafka生产配置
 */

@Configuration
@EnableKafka
public class KafkaProducerConfig {
	@Value("${kafka.producer.servers}")
	private String servers;
	// @Value("${kafka.producer.retries}")
	// private int retries;
	@Value("${kafka.producer.batch.size}")
	private int batchSize;
	@Value("${kafka.producer.linger}")
	private int linger;
	@Value("${kafka.producer.buffer.memory}")
	private int bufferMemory;
	@Value("${kafka.producer.enable.idempotence}")
	private boolean idempotence;
	
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
		// props.put(ProducerConfig.RETRIES_CONFIG, retries);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
		props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);// 默认为 byte[] 数组 
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);// 默认为 byte[] 数组
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence); // 幂等性，保证严格一次生产
		return props;
	}
	
	public ProducerFactory<String, String> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}
	
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<String, String>(producerFactory());
	}
}
