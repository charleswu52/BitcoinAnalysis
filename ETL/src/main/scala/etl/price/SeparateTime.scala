package etl.price

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * @author WuChao
 * @create 2020-12-22 21:21
 */

/**
 * 分割日期格式
 */

// 时间戳， 平均价格， 最低价格， 最高价格
// 年 月 日 时 分 星期
case class SeparateTimeBitcoin(timestamp: Long, currencyBTC: Double, currencyMony: Double, weightedPrice: Double,
                               yaer: String, month: String, day: String, hour: String, minutes: String, week: String)

object SeparateTime {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val url = DayPrice.getClass.getResource("/data/bitcoin.csv")
    val inputStream = env.readTextFile(url.getPath)

    val dataStream: DataStream[SeparateTimeBitcoin] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        // yyyy-MM-dd HH:mm 格式时间
        val timeString = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(arr(0).toLong * 1000L)

        // 单独获取 年 月 日 时 分
        val yaer = timeString.substring(0, 4)
        val month = timeString.substring(5, 7)
        val day = timeString.substring(8, 10)
        val hour = timeString.substring(11, 13)
        val minutes = timeString.substring(14, 16)

        // 获取星期 为坐标排序保持顺序 加入数字，使用周一作为一周的第一天
        val weekDays = Array[String]("周日", "周1", "周2", "周3", "周4", "周5", "周6")
        val cal = Calendar.getInstance()
        cal.setTimeInMillis(arr(0).toLong * 1000L)
        val week = weekDays(cal.get(Calendar.DAY_OF_WEEK) - 1)

        SeparateTimeBitcoin(arr(0).toLong * 1000L, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble,
          yaer, month, day, hour, minutes, week)
      })

    val filePath = "data_time/SeparatePrice.csv"
    val file = new File(filePath)
    if (file.exists()) {
      file.delete()
    }
    dataStream.writeAsCsv(filePath)
    env.execute()
  }
}

