package etl.sink

import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


/**
 * @author ngt
 * @create 2020-12-21 1:13
 */

case class RedisUtilsBitcoin(timestamp: String, weightedPrice: Double)

class MyRedisSink {
  // host 改成localhost
  val jedis = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build()

  case class MyRedisSinkFun() extends RedisMapper[RedisUtilsBitcoin] {
    override def getCommandDescription: RedisCommandDescription = {
      new RedisCommandDescription(RedisCommand.HSET, "Bitcoin")

    }

    override def getKeyFromData(data: RedisUtilsBitcoin): String = data.timestamp

    override def getValueFromData(data: RedisUtilsBitcoin): String = data.weightedPrice.toString
  }

}
