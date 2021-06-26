package etl.test

import etl.source.MyRedisSourceFun
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Properties

/**
 * @author WuChao
 * @create 2020-12-20 1:17
 */

/**
 * 测试 从 Redis 中读取数据 写入 Kafaka
 */
object RedisToKafka {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val prop = new Properties
    prop.load(ClassLoader.getSystemResourceAsStream("kafka.properties"))

    // 添加数据源来自 Redis
    val dataStream: DataStream[String] = env.addSource[String](new MyRedisSourceFun)
      .map(x => x)

    dataStream.print()


    // 数据写入 Kafka
    dataStream.addSink(
      new FlinkKafkaProducer[String]("192.168.100.102:9092,192.168.100.103:9092,192.168.100.104:9092",
        "bitcoin-source", new SimpleStringSchema())
    )

    env.execute("test read from redis to kafka job")
  }
}
