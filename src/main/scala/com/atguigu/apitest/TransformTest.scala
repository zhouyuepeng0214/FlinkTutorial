package com.atguigu.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object TransformTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    import org.apache.flink.api.scala._
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")

      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val keyedStream1: KeyedStream[SensorReading, Tuple] = dataStream
      .keyBy("id")

    //keyedStream1.print("keyedStream1")

//    val keyedStream1: KeyedStream[SensorReading, String] = dataStream
//      .keyBy(_.id)


    val dataStream2: DataStream[SensorReading] = keyedStream1
      .sum("temperature")
//    dataStream2.print("dataStream2")

    val dataStream3: DataStream[SensorReading] = keyedStream1.reduce((x,y) => SensorReading(x.id,x.time + 1,y.temperature + 10))

//    dataStream3.print("dataStream3")

    //多流转换

    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    val high: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("high","low")

//    high.print("high")
//    low.print("low")
//    all.print("all")

    val warning: DataStream[(String, Double)] = high.map(data => (data.id,data.temperature))

    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)

    //CoMap/CoFlatMap 是在connectedStream上执行Map/FlatMap
    val dataStream4: DataStream[Product] = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    //dataStream4.print()

    val unionStream: DataStream[SensorReading] = high.union(low)

    //3.自定义udf
    val dataStream5: DataStream[SensorReading] = dataStream.filter(new MyFilter())

    dataStream5.print()

    env.execute("transform")

  }

}

class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")

  }
}

class MyMapFunction extends RichMapFunction[Double,Int] {
  override def map(in: Double): Int = {
    in.toInt - 1
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}

class MyFlatMap extends RichFlatMapFunction[Int,(Int,Int)] {

  var subTaskIndex = 0

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
  }

  override def flatMap(in: Int, collector: Collector[(Int, Int)]): Unit = {
    if (in % 2 == subTaskIndex) {
      collector.collect((subTaskIndex,in))
    }
  }

  override def close(): Unit = {
    // 以下做一些清理工作，例如断开和HDFS的连接。
  }
}
