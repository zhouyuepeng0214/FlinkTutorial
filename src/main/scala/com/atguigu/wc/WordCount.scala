package com.atguigu.wc

import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    //执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputPath = "D:\\MyWork\\IdeaProjects\\spark\\FlinkTutorial\\src\\main\\resources\\wordcount.txt"

    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    val wordCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordCountDataSet.print()

  }

}
