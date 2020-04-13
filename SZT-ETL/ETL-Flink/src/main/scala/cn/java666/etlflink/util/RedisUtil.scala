package cn.java666.etlflink.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author Geek
 * @date 2020-03-30 00:59:39
 *
 */
object RedisUtil {
	// JedisPool资源池优化 - 最佳实践| 阿里云 https://www.alibabacloud.com/help/zh/doc-detail/98726.htm 
	private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
	jedisPoolConfig.setMaxTotal(200) //最大连接数
	jedisPoolConfig.setMaxIdle(20) //连接池中最大空闲的连接数
	jedisPoolConfig.setMinIdle(20) //最小空闲
	jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
	jedisPoolConfig.setMaxWaitMillis(2000) //忙碌时等待时长 毫秒
	jedisPoolConfig.setTestOnBorrow(false) //每次获得连接的进行测试
	private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, "localhost", 6379)
	
	// 直接得到一个 Redis 的连接
	def getJedisClient: Jedis = {
		jedisPool.getResource
	}
	
	//测试通过
	//def main(args: Array[String]): Unit = {
	//	println(getJedisClient.hget("szt:pageJson", "1"))
	//}
}
