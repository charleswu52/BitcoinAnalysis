package etl.price

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author WuChao
 * @create 2020-12-21 1:08
 */

/**
 * 计算 平均价格， 最低价格， 最高价格
 */

// 时间戳， 平均价格， 最低价格， 最高价格
case class outPrice(timestamp: Long, avgPrice: Double, minPrice: Double, maxPrice: Double)

class PriceFunc extends ProcessWindowFunction[(Long, Double), outPrice, Long, TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[(Long, Double)], out: Collector[outPrice]): Unit = {
    var sum: Double = 0.0
    val size = elements.size

    var minPrice: Double = Double.MaxValue
    var maxPrice: Double = Double.MinValue

    for (r <- elements) {
      sum += r._2
      minPrice = Math.min(minPrice, r._2)
      maxPrice = Math.max(maxPrice, r._2)
    }

    out.collect(outPrice(
      key, // 时间戳
      ((sum / size) * 10000).toInt / 10000.0, // 平均价格
      (minPrice * 10000).toInt / 10000.0, // 最低价格
      (maxPrice * 10000).toInt / 10000.0)) // 最高价格
  }
}

