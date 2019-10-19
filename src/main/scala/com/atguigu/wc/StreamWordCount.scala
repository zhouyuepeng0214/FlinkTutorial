package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import org.apache.flink.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //从外部命令中获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //关闭任务当中自动合并任务链的操作
//    env.disableOperatorChaining()

    val textDataStream: DataStream[String] = env.socketTextStream(host,port)


    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split("\\s")) //正则：空格\s
      .filter(_.nonEmpty)//.disableChaining() 把此算子单独拿出来生成任务链   .startNewChain() 从此算子开始形成新的任务链
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    wordCountDataStream.print().setParallelism(1)

    env.execute("Socket stream word count")



  }

}
