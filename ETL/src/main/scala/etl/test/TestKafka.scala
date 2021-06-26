package etl.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

/**
 * @author WuChao
 * @create 2020-12-31 16:47
 */

/**
 * 测试: 从Kafka中读取数据
 */
object TestKafka {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "bitcoin")

    // 使用通配符 同时匹配多个Kafka主题
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String](
      java.util.regex.Pattern.compile("bitcoin-source"), new SimpleStringSchema(), properties))

    inputStream.print()
    env.execute("test kafka connector job")
  }
}

