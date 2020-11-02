package DataSet.wordcount

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //1. 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2. source:获取数据源
    val pathURL: URL = getClass.getResource("/DataSet/data/words.txt")
    val sourceDS: DataStream[String] = env.readTextFile(pathURL.getPath)
//    val sourceDS: DataStream[String] = env.socketTextStream("hadp01", 7777)
    //3. transformation:处理数据
    val resultDS: DataStream[(String, Int)] = sourceDS.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    //4. sink:指定结果位置
    resultDS.print()
    //5. 启动程序
    env.execute("wordcount job")
  }

}
