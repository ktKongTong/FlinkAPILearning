import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.net.URL

object AverageTemp {
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //获取数据源
    val pathURL: URL = getClass.getResource("/sensordata.txt")
    val sourceDS: DataStream[String] = env.readTextFile(pathURL.getPath)
    //处理数据
    val resultDS=sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1).toFloat, arr(2).toLong)
    })
      .keyBy(0)
      .countWindow(5)
      .sum(1)
      .map(data => {
      data._2/5
    })
    //指定结果位置
    resultDS.print()
    //启动程序
    env.execute("job")
  }
}
