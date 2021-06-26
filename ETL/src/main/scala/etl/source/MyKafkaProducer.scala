package etl.source

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.io.BufferedSource

/**
 * @author WuChao
 * @create 2020-12-20 13:42
 */

/**
 * 数据源
 * 向Kafka 写入数据
 */
object MyKafkaProducer {

  def main(args: Array[String]): Unit = {
    writeToKafka("bitcoin-source0")
  }

  def writeToKafka(topic: String): Unit = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)


    val url = MyKafkaProducer.getClass.getResource("/data/bitstampUSD.csv")
    val source: BufferedSource = io.Source.fromFile(url.getPath)

    for (line <- source.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
      // 模拟写入延迟
            TimeUnit.MILLISECONDS.sleep(1000)
    }
    producer.close()
  }

}
