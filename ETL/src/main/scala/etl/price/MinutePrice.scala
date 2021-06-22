package etl.price

import org.apache.flink.streaming.api.scala._

import java.io.File
import java.text.SimpleDateFormat

/**
 * @author ngt
 * @create 2020-12-20 20:30
 *         统计每分钟的交易价格
 */

// 时间戳 交易价格
case class MinutePrice_Bitcoin(timestamp: String, weightedPrice: Double)

object MinutePrice {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("data/bitcoin.csv")

    val dataStream: DataStream[MinutePrice_Bitcoin] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        MinutePrice_Bitcoin(new SimpleDateFormat("yyyy-MM-dd HH:mm").format(arr(0).toLong * 1000L), (arr(7).toDouble * 10000).toInt / 10000.0)
      })

    val filePath = "data_time/MinutePrice.csv"
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }
    dataStream.writeAsCsv(filePath)
    env.execute("MinutePrice")
  }
}
