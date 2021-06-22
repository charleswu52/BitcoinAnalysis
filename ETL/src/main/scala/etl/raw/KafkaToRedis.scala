package etl.raw

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector

import java.util.Properties

/**
 * @author WuChao
 * @create 2021/6/17 13:50
 */

// 时间戳(秒)，开盘价格，最高价，最低价，收盘价， 交易量， 交易价值(美元)， 权重交易价格
case class CSVToRedis_Bitcoin(timestamp: Long, openPrice: String, highPrice: String, lowPrice: String, closePrice: String,
                              currencyBTC: String, currencyValue: String, weightedPrice: String)

object KafkaToRedis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "bitcoin")
    properties.setProperty("flink.partition-discovery.interval-millis", "1000")

    // 使用通配符 同时匹配多个Kafka主题
    val inputStream: DataStream[String] =
      env.addSource(new FlinkKafkaConsumer011[String](java.util.regex.Pattern.compile("bitcoin-source[0-9]"), new SimpleStringSchema(), properties))

    val dataStream: DataStream[CSVToRedis_Bitcoin] = inputStream
      .filter(data => {
        !data.contains("NaN") && !data.contains("Timestamp")
      })
      .map(data => {
        val arr = data.split(",")
        //        CSVToRedis_Bitcoin(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(arr(0).toLong * 1000L),
        //          arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
        CSVToRedis_Bitcoin(arr(0).toLong,
          arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
      })


    // host 改成localhost
    val jedis = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build()

    dataStream.addSink(new RedisSink(jedis, MyRedisSinkFun()))

    env.execute("KafkaToRedis")
  }
}

class PriceFunc extends ProcessAllWindowFunction[CSVToRedis_Bitcoin,CSVToRedis_Bitcoin,TimeWindow]{
  override def process(context: Context, elements: Iterable[CSVToRedis_Bitcoin], out: Collector[CSVToRedis_Bitcoin]): Unit = {
    for(i <- elements){
      out.collect(i)
    }
  }
}

case class MyRedisSinkFun() extends RedisMapper[CSVToRedis_Bitcoin] {
  /**
   * Returns descriptor which defines data type.
   *
   * @return data type descriptor
   */
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "Bitcoin")
  }

  /**
   * Extracts key from data.
   *
   * @param data source data
   * @return key
   */
  override def getKeyFromData(data: CSVToRedis_Bitcoin): String = data.timestamp.toString

  /**
   * Extracts value from data.
   *
   * @param data source data
   * @return value
   */
  override def getValueFromData(data: CSVToRedis_Bitcoin): String = data.toString
}


/*
    Redis  集群配置
    val nodes: util.HashSet[InetSocketAddress] = new util.HashSet()
    nodes.add(new InetSocketAddress("127.0.0.1", 7001))
    nodes.add(new InetSocketAddress("127.0.0.1", 7002))
    nodes.add(new InetSocketAddress("127.0.0.1", 7003))
    nodes.add(new InetSocketAddress("127.0.0.1", 7004))
    nodes.add(new InetSocketAddress("127.0.0.1", 7005))
    nodes.add(new InetSocketAddress("127.0.0.1", 7006))
val jedisCluster: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build()
 */
