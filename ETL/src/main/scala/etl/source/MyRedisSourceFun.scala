package etl.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.Jedis

import java.util

/**
 * @author WuChao
 * @create 2020-12-20 13:43
 */

/**
 *从Redis中消费数据
 */
case class MyRedisSourceFun()extends RichSourceFunction[String] {

  var client: Jedis = _

  override def open(parameters: Configuration): Unit = {
    client = RedisUtil.getJedisClient
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    open(new Configuration)

    val stringToString: util.Map[String, String] = client.hgetAll("Bitcoin")

    val strings: util.Collection[String] = stringToString.values()
    val value: util.Iterator[String] = strings.iterator()
    while (value.hasNext){
      ctx.collect(value.next())
    }

  }


  override def cancel(): Unit = close()

  override def close(): Unit = client.close()
}

