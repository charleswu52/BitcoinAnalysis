package time_statists

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import java.text.SimpleDateFormat

/**
 * @author ngt
 * @create 2020-12-20 19:50
 */

// 时间戳(秒)，开盘价格，最高价，最低价，收盘价， 交易量， 交易价值(美元)， 权重交易价格
case class Bitcoin(timestamp: Long, openPrice: Double, highPrice: Double, lowPrice: Double, closePrice: Double,
                   currencyBTC: Double, currencyValue: Double, weightedPrice: Double)

case class MostPrice(time: Long, price: Double)

case class MostPriceOut(time: String, price: Double)

object MinMaxPrice {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val inputStream: DataStream[String] = env.readTextFile("data/test.csv")

    var theHour: Long = 0L
    val dataStream: DataStream[MostPrice] = inputStream.
      map(data => {
        val arr: Array[String] = data.split(",")
        //        theHour = ((arr(0).toLong * 1000L) / (1000 * 60 * 60 * 24)) * (1000 * 60 * 60 * 24)
        MostPrice(arr(0).toLong * 1000L, arr(7).toDouble)
      }).assignAscendingTimestamps(_.time)

    val minPriceDataStream: DataStream[(Long, Double, Double, Double)] = dataStream
      .map(r => (r.time, r.price))
      .keyBy(_._1)
      .timeWindow(Time.hours(24), Time.hours(1))
      .process(new MinMaxPriceFunc)


    val result: DataStream[(String, Double, Double, Double)] = minPriceDataStream
      .map(data => {
        ((new SimpleDateFormat("yyyy-MM-dd HH:mm").format(data._1), data._2, data._3, data._4))
      })
    result.print()
    env.execute()

  }

}
