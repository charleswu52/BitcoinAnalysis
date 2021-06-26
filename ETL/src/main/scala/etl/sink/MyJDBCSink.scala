package etl.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import java.sql.{Connection, Date, DriverManager, PreparedStatement}


/**
 * @author WuChao
 * @create 2020-12-21 1:45
 */

/**
 * 向 MySQL 中写入数据
 */
case class JDBCBitcion(timestamp: Date, weightedPrice: Double)

class MyJDBCSink extends RichSinkFunction[JDBCBitcion] {
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://192.168.100.102:3306/test", "root", "123456")
    insertStmt = conn.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)")
    updateStmt = conn.prepareStatement("update sensor_temp set temp = ? where id = ?")
  }

  override def invoke(value: JDBCBitcion): Unit = {
    // 先执行更新操作，查到就更新
    updateStmt.setDate(1, value.timestamp)
    updateStmt.setDouble(2, value.weightedPrice)
    updateStmt.execute()
    // 如果更新没有查到数据，那么就插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setDate(1, value.timestamp)
      updateStmt.setDouble(2, value.weightedPrice)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}

