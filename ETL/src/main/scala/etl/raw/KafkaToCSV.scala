package etl.raw

import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.sources.tsextractors.TimestampExtractor

import java.io.File
import java.util.Properties


/**
 * @author WuChao
 * @create 2021/6/17 13:44
 * 数据清洗，去除原始文件中的无效数据，保存到本地
 */

// 时间戳(秒)，开盘价格，最高价，最低价，收盘价， 交易量， 交易价值(美元)， 权重交易价格
case class File2CSV_Bitcoin(timestamp: Long, openPrice: String, highPrice: String, lowPrice: String, closePrice: String,
                            currencyBTC: String, currencyValue: String, weightedPrice: String)

object DataCleansingToCSV {
  def main(args: Array[String]): Unit = {
    // 创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    // Kafka 配置
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "bitcoin")
    properties.setProperty("flink.partition-discovery.interval-millis", "1000")

    // 创建 Kafka consumer
    // 使用通配符 同时匹配多个 Kafka主题
    val consumer = new FlinkKafkaConsumer[String](java.util.regex.Pattern.compile("bitcoin-source[0-9]"), new SimpleStringSchema(), properties)
    // 从 topic的开始读取数据，实际生产过程中 设置 从 offset读取或者 读取实时产生的
    consumer.setStartFromEarliest()
    val inputStream: DataStream[String] = env.addSource(consumer)

    // 进行 filter map 处理
    val dataStream : DataStream[File2CSV_Bitcoin] = inputStream
      .filter(data => {
        !data.contains("NaN") && !data.contains("Timestamp")
      })
      .map(data => {
        val arr: Array[String] = data.split(",")
        File2CSV_Bitcoin(arr(0).toLong * 1000L, arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
      })

    val dataStreamWithTime = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[File2CSV_Bitcoin](Time.milliseconds(1000)) {
      override def extractTimestamp(element: File2CSV_Bitcoin): Long = element.timestamp
    })

//    dataStream.timeWindowAll(Time.seconds(5)).apply()



    val filePath = "E:\\WorkSpace\\InterviewProject\\source\\BitcoinAnalysis\\ETL\\data\\bitcoin2.csv"  //测试
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }

    // 弃用的方法
//    dataStream.writeAsCsv(filePath)
    // 重新参考的方法 也不牢靠
//    dataStream.addSink(StreamingFileSink.forRowFormat(new Path(filePath),new SimpleStringEncoder[File2CSV_Bitcoin]()).build())



    dataStream.print("Filtered bitcoin")

    env.execute("DataCleansingToCSV")
  }

  case class MyCSVSink() extends RichSinkFunction[File2CSV_Bitcoin]{

  }
}
