package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


import org.apache.flink.util.Collector

object SideOutputTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream: DataStream[String] = env.socketTextStream("hadoop110",7777)

    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.time * 1000
    })
    dataStream.print("input data")

    val processStream: DataStream[SensorReading] = dataStream.keyBy(_.id)
      .process(new FreezinAlert())

    processStream.print("process data")
    processStream.getSideOutput(new OutputTag[String]("freezingAlertOutput")).print("alert data")

    env.execute()

  }

}

//小于32，输出报警信息到测输出流
class FreezinAlert() extends ProcessFunction[SensorReading,SensorReading]{

  lazy val freezingAlertOutput : OutputTag[String] = new OutputTag[String]("freezingAlertOutput")

  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {

    if (i.temperature < 32.0) {
      context.output(freezingAlertOutput,"freezingAlertOutput for" + i.id)
    } else {
      collector.collect(i)

    }
  }
}
