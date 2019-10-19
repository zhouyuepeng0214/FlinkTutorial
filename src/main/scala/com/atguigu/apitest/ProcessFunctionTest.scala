package com.atguigu.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.rest.messages.checkpoints.SubtaskCheckpointStatistics.CompletedSubtaskCheckpointStatistics.CheckpointAlignment
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
    val chkpointConf: CheckpointConfig = env.getCheckpointConfig
    chkpointConf.setCheckpointTimeout(10000)
    chkpointConf.setFailOnCheckpointingErrors(false)
    chkpointConf.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000))


    val stream: DataStream[String] = env.socketTextStream("hadoop110", 7777)

    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.time * 1000
      })
    dataStream.print("input data")

    val keyedStream: KeyedStream[SensorReading, String] = dataStream.keyBy(_.id)
    //    val processStream: DataStream[String] = keyedStream.process(new TempIncreAlert())
    //    processStream.print("process data")


    //    val processedStream1: DataStream[(String, Double, Double)] = keyedStream.flatMap(new TempAlert(10))
    //    processedStream1.print("flatMap test")

    val processedStream2: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
      case (in: SensorReading, None) => (List.empty, Some(in.temperature))
      case (in: SensorReading, lastTemp: Some[Double]) => {
        val diff = (in.temperature - lastTemp.get).abs
        if (diff > 10) {
          (List((in.id, in.temperature, lastTemp.get)), Some(in.temperature))
        } else {
          (List.empty, Some(in.temperature))
        }
      }
    }
    processedStream2.print("processedStream2 data")


    env.execute()

  }

}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp: Double = lastTemp.value()
    //更新温度值
    lastTemp.update(i.temperature)

    val curTimerTs: Long = currentTimer.value()

    if (i.temperature < preTemp || curTimerTs == 0) {
      // 如果温度下降，或是第一条数据，删除定时器并清空状态
      val timerTs: Long = context.timerService().currentProcessingTime() + 10000L
      context.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    } else if (preTemp > i.temperature || preTemp == 0.0) {
      // 温度上升且没有设过定时器，则注册定时器
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }

  //回调函数
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + "温度连续上升")
    currentTimer.clear()
  }
}

class TempAlert(thr: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  //先定义空的状态变量
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //声明state变量
    lastTempState = getRuntimeContext.getState[Double](new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp = lastTempState.value()

    // 跟最新的温度值计算差值，如果大于阈值，那么输出报警
    val diff = (in.temperature - lastTemp).abs
    if (diff > thr) {
      collector.collect((in.id, in.temperature, lastTemp))
    }
    lastTempState.update(in.temperature)

  }

}