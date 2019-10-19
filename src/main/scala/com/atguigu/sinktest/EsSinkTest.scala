package com.atguigu.sinktest

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object EsSinkTest {

  def main(args: Array[String]): Unit = {

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop110",9200))

    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val json = new util.HashMap[String, String]()

          json.put("sensor_id", t.id)
          json.put("timestamp", t.time.toString)
          json.put("sensor_id", t.temperature.toString)

          val indexRequest: IndexRequest = Requests.indexRequest().index("sensor").`type`("data").source(json)

          requestIndexer.add(indexRequest)
          println("save data" + t + "successfully")

        }
      }
    )


//    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
//
//    )

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[String] = env.readTextFile("D:\\MyWork\\IdeaProjects\\flink\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)

    })


    dataStream.addSink(esSinkBuilder.build())


    dataStream.print()

    env.execute("es sink test")

  }

}
