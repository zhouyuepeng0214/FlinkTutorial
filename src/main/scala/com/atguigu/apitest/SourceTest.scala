package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random

object SourceTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    val stream2: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val stream3: DataStream[Any] = env.fromElements(1,2.0,"string")

    //从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop110:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream4: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    //stream4.print()


    //stream3.print("stream3").setParallelism(6)

    //自定义source
    val dataStream4: DataStream[SensorReading] = env.addSource(new SensorSource())

    dataStream4.print()

    env.execute("source test")

  }

}


case class SensorReading(id : String,time : Long,temperature : Double) {}

class SensorSource extends SourceFunction[SensorReading]() {

  // 定义一个flag，表示数据源是否正常运行
  var running : Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {

    val random: Random = new Random()

    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(i => ("sensor_" + i,60 + random.nextGaussian() * 20))

    // 用无限循环，产生数据流
    while(running) {
      curTemp = curTemp.map(t => (t._1,t._2 + random.nextGaussian()))

      val time: Long = System.currentTimeMillis()

      curTemp.foreach(t => sourceContext.collect(SensorReading(t._1,time,t._2)))

      Thread.sleep(500)

    }

  }

  override def cancel(): Unit = {
    running = false
  }
}