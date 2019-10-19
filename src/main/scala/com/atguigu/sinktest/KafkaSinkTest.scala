package com.atguigu.sinktest

import java.util.Properties

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.api.scala._

object KafkaSinkTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

//    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop110:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))



    val dataStream: DataStream[String] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")

      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
    })

    //dataStream.print()

    val value: DataStreamSink[String] = dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop110:9092","test",new SimpleStringSchema()))

    env.execute("kafka sink")

  }

}
