package DataSet.wordcount

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    //1. 获取运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2. source:获取数据源
    val pathURL: URL = getClass.getResource("/DataSet/data/words.txt")
    val sourceDS: DataSet[String] = env.readTextFile(pathURL.getPath)
    //3. transformation:处理数据
    val resultDS: AggregateDataSet[(String, Int)] = sourceDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    //4. sink:指定结果位置
    resultDS.print()
  }
}
