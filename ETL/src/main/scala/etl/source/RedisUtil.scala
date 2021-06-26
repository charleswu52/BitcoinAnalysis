package etl.source

import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool, JedisPoolConfig}

import java.util

/**
 * @author WuChao
 * @create 2020-12-20 13:44
 */

/**
 * Redis 工具类
 * 获取连接
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

  private val jedisPool: JedisPool = new JedisPool(jedisPoolConfig, "127.0.0.1", 6379)

  // 直接得到一个 Redis 的连接
  def getJedisClient: Jedis = {
    jedisPool.getResource
  }



  // Redis集群使用, 需要 jedis 2.9 及以上版本
  val timeOut = 10000
  val nodes: util.Set[HostAndPort] = new util.HashSet[HostAndPort]
  nodes.add(new HostAndPort("127.0.0.1", 7001))
  nodes.add(new HostAndPort("127.0.0.1", 7002))
  nodes.add(new HostAndPort("127.0.0.1", 7003))
  nodes.add(new HostAndPort("127.0.0.1", 7004))
  nodes.add(new HostAndPort("127.0.0.1", 7005))
  nodes.add(new HostAndPort("127.0.0.1", 7006))
  private val cluster: JedisCluster = new JedisCluster(nodes, timeOut, jedisPoolConfig)
}