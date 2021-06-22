package etl.price

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.io.File
import java.text.SimpleDateFormat

/**
 * @author ngt
 * @create 2020-12-20 20:59
 */

case class HourPrice_Bitcoin(timestamp: Long, weightedPrice: Double)

case class HourPriceString_Bitcoin(timestamp: String, weightedPrice: Double)

//case class HourPrice_Raw(timestamp: String, currencyBTC: Double, currencyValue: Double, weightedPrice: Double)

object HourPrice {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("data/bitcoin.csv")

    // 表示整点时间
    var theHour: Long = 0L
    //    theHour = ((arr(0).toLong * 1000L) / (1000 * 60 * 60)) * (1000 * 60 * 60)
    val dataStream: DataStream[HourPrice_Bitcoin] = inputStream.
      map(data => {
        val arr: Array[String] = data.split(",")
        theHour = ((arr(0).toLong * 1000L) / (1000 * 60 * 60)) * (1000 * 60 * 60)
        HourPrice_Bitcoin(theHour, arr(7).toDouble)
      }).assignAscendingTimestamps(_.timestamp)

    //    dataStream.print()
    val avgPriceDataStream: DataStream[outPrice] = dataStream
      .map(r => (r.timestamp, r.weightedPrice))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60))
      .process(new PriceFunc)

    val result: DataStream[(String, Double, Double, Double)] = avgPriceDataStream
      .map(data => {
        (new SimpleDateFormat("yyyy-MM-dd HH:mm").format(data.timestamp), data.avgPrice, data.minPrice, data.maxPrice)
      })

    val filePath = "data_time/HourPrice.csv"
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }
    result.writeAsCsv(filePath)
    env.execute("HourPrice")
  }

}

