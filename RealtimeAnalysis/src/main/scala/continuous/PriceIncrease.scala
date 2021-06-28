package continuous

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Properties

/**
 * @author WuChao
 * @create 2020-12-22 5:56
 */
case class Bitcoin(key: Long, time: Long, price: Double)

object PriceIncrease {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "consumer-group")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("bitcoin", new SimpleStringSchema(), properties))


    //    val inputStream: DataStream[String] = env.readTextFile("data/bitcoin.csv")

    val dataStream: DataStream[Bitcoin] = inputStream.
      map(data => {
        val strings: Array[String] = data.split(",")
        Bitcoin(0L, strings(0).toLong * 1000L, strings(7).toDouble)
      })
      .assignAscendingTimestamps(_.time)

    //    dataStream.print()
    val value = dataStream
      .keyBy(_.key)
      .process(new IncreWarning1(10 * 60 * 1000L))

    value.print()
    env.execute("PriceIncrease")
  }
}


class IncreWarning1(interval: Long) extends KeyedProcessFunction[Long, Bitcoin, String] {
  lazy val lastPriceState: ValueState[Double] =
    getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-price", classOf[Double]))

  lazy val timerTsState: ValueState[Long] =
    getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  var dataTime: Long = _

  override def processElement(i: Bitcoin, context: KeyedProcessFunction[Long, Bitcoin, String]#Context, collector: Collector[String]): Unit = {
    //先取出状态

    //    timerTsState.update(i.time)
    val lastPrice: Double = lastPriceState.value()
    val timerTs: Long = timerTsState.value()

    lastPriceState.update(i.price)

    if (i.price > lastPrice && timerTs == 0L) {
      val ts: Long = context.timerService().currentProcessingTime() + interval
      context.timerService().registerProcessingTimeTimer(ts)
      timerTsState.update(ts)
    } else if (i.price < lastPrice) {
      context.timerService().deleteProcessingTimeTimer(timerTs)
      timerTsState.clear()
    }
    dataTime = i.time
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Bitcoin, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(new SimpleDateFormat("yyyy-MM-dd HH:mm").format(dataTime) + interval / 1000 + " 秒连续上升")
    timerTsState.clear()
  }
}


