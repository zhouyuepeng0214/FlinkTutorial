package com.atguigu.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.apitest.{SensorReading, SensorSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val stream2: DataStream[SensorReading] = env.addSource(new SensorSource())

    stream2.addSink(new MyJdbcSink())

    stream2.print("stream2")

    env.execute("Mysql sink test")

  }

}

class MyJdbcSink() extends RichSinkFunction[SensorReading] {

  // 定义连接、PreparedStatement
  var conn : Connection = _
  var insertStmt : PreparedStatement = _
  var updateStmt : PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb2","root","123456")
    insertStmt = conn.prepareStatement("INSERT INTO temperature(sensor,temp) VALUES(?,?)")
    updateStmt = conn.prepareStatement("UPDATE  temperature SET temp = ? WHERE sensor = ?")

  }

  // 对每一条数据，调用连接执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
//    super.invoke(value, context)
    // 执行更新
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()

    // 如果刚才没有更新数据，那么执行插入
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }

  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
