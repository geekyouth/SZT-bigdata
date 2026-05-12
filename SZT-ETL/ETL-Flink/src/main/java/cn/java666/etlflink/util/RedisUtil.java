package cn.java666.etlflink.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static final JedisPoolConfig poolConfig = new JedisPoolConfig();
    private static final JedisPool jedisPool;

    static {
        poolConfig.setMaxTotal(200);
        poolConfig.setMaxIdle(20);
        poolConfig.setMinIdle(20);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(java.time.Duration.ofSeconds(2).toMillis());
        poolConfig.setTestOnBorrow(false);
        jedisPool = new JedisPool(poolConfig, "localhost", 6379);
    }

    public static Jedis getJedisClient() {
        return jedisPool.getResource();
    }
}
