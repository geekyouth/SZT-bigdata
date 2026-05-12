package cn.java666.etlspringboot.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class RedisService {

    private final StringRedisTemplate redis;

    public RedisService(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public String get(String key) {
        Object val = redis.opsForHash().get("szt:pageJson", key);
        return val != null ? val.toString() : null;
    }
}
