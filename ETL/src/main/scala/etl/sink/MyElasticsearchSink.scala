package etl.sink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

/**
 * @author WuChao
 * @create 2020-12-20 21:04
 */

/**
 * 向 ElasticSearch中写入数据
 */
case class ElasticsearchBitcion(timestamp: String, avgPrice: String, minPrice: String, maxPrice: String)

/*
  不支持带参数的构造函数会抛出异常：
  The implementation of the provided ElasticsearchSinkFunction is not serializable. The object probabl.
 */
class MyElasticsearchSink {
  val httpHosts = new java.util.ArrayList[HttpHost]
  //  httpHosts.add(new HttpHost("192.168.100.102", 9200, "http"))
  //  httpHosts.add(new HttpHost("192.168.100.103", 9200, "http"))
  //  httpHosts.add(new HttpHost("192.168.100.104", 9200, "http"))
  // 本地ES
  httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))

  val esSinkFunc = new ElasticsearchSinkFunction[ElasticsearchBitcion] {
    override def process(element: ElasticsearchBitcion, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
      // 包装写入es的数据
      val dataSource = new util.HashMap[String, String]()

      // 每分钟价格
      //      dataSource.put("Time_Minute", element.timestamp)
      //      dataSource.put("Price_Minute", element.weightedPrice)

      // 每天价格
      //      dataSource.put("Time_Day", element.timestamp)
      //      dataSource.put("avgPrice_Day", element.avgPrice)
      //      dataSource.put("minPrice_Day", element.minPrice)
      //      dataSource.put("maxPrice_Day", element.maxPrice)

      // 每小时价格
      dataSource.put("Time_Hour", element.timestamp)
      dataSource.put("avgPrice_Hour", element.avgPrice)
      dataSource.put("minPrice_Hour", element.minPrice)
      dataSource.put("maxPrice_Hour", element.maxPrice)

      // 创建一个index request
      val indexRequest = Requests.indexRequest()
        // 不支持，带参数构造函数，因此需要在此修改 index 的值
        .index("original-hour")
        .source(dataSource)

      indexer.add(indexRequest)
    }
  }
}


