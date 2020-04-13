package cn.java666.etlspringboot.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * @author Geek
 * @date 2020-04-13 21:39:41
 *
 * SpringBoot高级篇Redis之Hash数据结构使用姿势 - 掘金 https://juejin.im/post/5c1399a7f265da61764ac526
 */

@Service
public class RedisService {
	
	@Resource
	private StringRedisTemplate redis; // 使用默认的 redis 序列化
	
	public String get(String key) {
		return redis.opsForHash().get("szt:pageJson", key).toString();
	}
	
	// 暂时用不上
	private void set(String key, String value) {
		redis.opsForHash().put("test:xxx", key, value);
	}
}
