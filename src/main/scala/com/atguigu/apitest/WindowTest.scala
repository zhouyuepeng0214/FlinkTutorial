package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.getConfig.setAutoWatermarkInterval(100L)

//    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val stream: DataStream[String] = env.socketTextStream("hadoop110",7777)

    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    //.assignAscendingTimestamps(_.time * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.time * 1000
    })


    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    dataStream.print("input data")
    minTempPerWindowStream.print("min temp")

    dataStream.keyBy(_.id)
        .process(new MyProcess())

    env.execute()

  }

}

//class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
//
//  val bound = 6000
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)
//
//  override def extractTimestamp(t: SensorReading, l: Long): Long = {
//    maxTs = maxTs.max(t.time * 1000)
//    t.time * 1000
//  }
//}

class MyAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = new Watermark(l)

  override def extractTimestamp(t: SensorReading, l: Long): Long = t.time * 1000
}

class MyProcess() extends KeyedProcessFunction[String,SensorReading,String] {
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    context.timerService().registerEventTimeTimer(2000)
  }
}

