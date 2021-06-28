package cep

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util

/**
 * @author WuChao
 * @create 2020-12-22 15:16
 */

case class ValueCEPBitcoin(t: String, time: Long, currencyBTC: Double, currencyValue: Double, price: Double)

object ValueCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
    val inputStream: DataStream[String] = env.readTextFile("data/bitcoin.csv")

    val dataStream: DataStream[ValueCEPBitcoin] = inputStream.
      map(data => {
        val arr: Array[String] = data.split(",")
        //        theHour = ((arr(0).toLong * 1000L) / (1000 * 60 * 60 * 24)) * (1000 * 60 * 60 * 24)
        ValueCEPBitcoin("1", arr(0).toLong * 1000L, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble)
      }).assignAscendingTimestamps(_.time)

    val value: Pattern[ValueCEPBitcoin, ValueCEPBitcoin] = Pattern
      .begin[ValueCEPBitcoin](name = "begin").where(_.price > 2000)
      .followedBy("next").where(_.price > 2000)
      .timesOrMore(10)

    val value1: Pattern[ValueCEPBitcoin, ValueCEPBitcoin] = Pattern
      .begin[ValueCEPBitcoin](name = "begin")
      .timesOrMore(6)
      .where(_.price > 2000)

    val patternStream: PatternStream[ValueCEPBitcoin] = CEP.pattern(dataStream, value1)

    patternStream.select(new priceEventMatch).print()
    env.execute()
  }
}

case class ResultBitcoin(startvalueCEPBitcoin: ValueCEPBitcoin, endvalueCEPBitcoin: ValueCEPBitcoin, count: Int)


class priceEventMatch1() extends PatternProcessFunction[ValueCEPBitcoin, ResultBitcoin] {
  override def processMatch(map: util.Map[String, util.List[ValueCEPBitcoin]], context: PatternProcessFunction.Context, collector: Collector[ResultBitcoin]): Unit = {
    println("test")
    var endTime: Long = 0L
    var endCurrencyBTC: Double = 0.0
    var endCurrencyValue: Double = 0.0
    var endPrice: Double = 0.0

    var count: Int = 0
    while (map.get("begin").iterator().hasNext) {
      endTime = map.get("begin").iterator().next().time
      endCurrencyBTC = map.get("begin").iterator().next().currencyBTC
      endCurrencyValue = map.get("begin").iterator().next().currencyValue
      endPrice = map.get("begin").iterator().next().price
      count += 1
    }
    ResultBitcoin(map.get("begin").get(0), ValueCEPBitcoin("t", endTime, endCurrencyBTC, endCurrencyValue, endPrice), count)
  }
}

class priceEventMatch() extends PatternSelectFunction[ValueCEPBitcoin, ResultBitcoin] {
  override def select(map: util.Map[String, util.List[ValueCEPBitcoin]]): ResultBitcoin = {
    var endTime: Long = 0L
    var endCurrencyBTC: Double = 0.0
    var endCurrencyValue: Double = 0.0
    var endPrice: Double = 0.0

    var count: Int = 0
    while (map.get("begin").iterator().hasNext) {
      endTime = map.get("begin").iterator().next().time
      endCurrencyBTC = map.get("begin").iterator().next().currencyBTC
      endCurrencyValue = map.get("begin").iterator().next().currencyValue
      endPrice = map.get("begin").iterator().next().price
      count += 1
    }

    ResultBitcoin(map.get("begin").get(0), ValueCEPBitcoin("t", endTime, endCurrencyBTC, endCurrencyValue, endPrice), count)
  }
}