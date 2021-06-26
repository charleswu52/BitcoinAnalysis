package etl.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
 * @author WuChao
 * @create 2020-12-19 21:58
 */

/**
 * 数据清洗 过程测试
 * 从 文件 | Kafka 中读取数据
 */
object DataCleansing {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从 Kafka集群中读取数据
    //    val properties: Properties = new Properties()
    //    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //    properties.setProperty("group.id", "consumer-group")
    //
    //    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("bitcoin-source", new SimpleStringSchema(), properties))

    // 从文件中读取数据
    val url = DataCleansing.getClass.getResource("/data/test.csv")
    val inputStream = env.readTextFile(url.getPath)

    // 对数据进行清洗
    val dataStream: DataStream[String] = inputStream
      .filter(data => {
        !data.contains("NaN")
      })

    //      .map(arr => {
    //        Bitcoin(arr(0).toLong, arr(1).toDouble, arr(2).toDouble,
    //          arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble)
    //      })

    // Redis集群
    //    val nodes: util.HashSet[InetSocketAddress] = new util.HashSet()
    //    nodes.add(new InetSocketAddress("192.168.31.8", 7001))
    //    nodes.add(new InetSocketAddress("192.168.31.8", 7002))
    //    nodes.add(new InetSocketAddress("192.168.31.8", 7003))
    //    nodes.add(new InetSocketAddress("192.168.31.8", 7004))
    //    nodes.add(new InetSocketAddress("192.168.31.8", 7005))
    //    nodes.add(new InetSocketAddress("192.168.31.8", 7006))
    //val jedisCluster: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build()

    //    val jedis = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

    // 清洗后的数据写入 Redis | Kafka
    //    dataStream.addSink(new RedisSink(jedis, new MyRedisSinkFun))
    //    dataStream.addSink(new FlinkKafkaProducer011("hadoop102:9092", "sinktest", new SimpleStringSchema()))
    dataStream.addSink(new FlinkKafkaProducer[String](
      "hadoop102:9092,hadoop103:9092,hadoop104:9092", "bitcoin-source", new SimpleStringSchema()))

    dataStream.print()

    env.execute("data cleaning job")

  }
}
