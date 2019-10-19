package com.atguigu.sinktest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RdisSinkTest {

  def main(args: Array[String]): Unit = {

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop110").setPort(6379).build()


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    import org.apache.flink.api.scala._

    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")

      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    dataStream.addSink(new RedisSink[SensorReading](config,MyRedisMapper()))

    dataStream.print()
    env.execute("redis sink test")
  }

}

case class MyRedisMapper() extends RedisMapper[SensorReading] {
  // 保存到redis的命令的描述，HSET key field value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }

  override def getKeyFromData(t: SensorReading): String = {
    t.id
  }

  override def getValueFromData(t: SensorReading): String = {
    t.temperature.toString
  }
}
