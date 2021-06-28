package time_statists

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author WuChao
 * @create 2020-12-21 18:40
 */
class MinMaxPriceFunc extends ProcessWindowFunction[(Long, Double), (Long, Double, Double, Double), Long, TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[(Long, Double)],
                       out: Collector[(Long, Double, Double, Double)]): Unit = {
    var minPrice: Double = Double.MaxValue
    var maxPrice: Double = Double.MinValue
    for (r <- elements) {
      minPrice = Math.min(minPrice, r._2)
      maxPrice = Math.max(maxPrice, r._2)
    }
    out.collect((key,
      (minPrice * 10000).toInt / 10000.0,
      (maxPrice * 10000).toInt / 10000.0,
      (10000 * maxPrice / minPrice).toInt / 10000.0))
  }
}