package etl.source

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.BufferedSource

/**
 * @author WuChao
 * @create 2021/6/17 13:42
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
    //    val source: BufferedSource = io.Source.fromFile("data/bitcoin.csv")
    val source: BufferedSource = io.Source.fromFile("E:\\WorkSpace\\InterviewProject\\source\\BitcoinAnalysis\\ETL\\data\\bitstampUSD.csv")

    for (line <- source.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
      //      TimeUnit.MILLISECONDS.sleep(10)
    }
    producer.close()
  }

}
