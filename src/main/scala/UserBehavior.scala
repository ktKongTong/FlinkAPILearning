import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

import java.net.URL

object UserBehavior {
//  用户 Id、商品 Id、商品类别 Id，用户行为和时间戳
  def main(args: Array[String]): Unit = {
    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //获取数据
    val pathURL: URL = getClass.getResource("/UserBehavior.csv")
    val sourceDS = env.readTextFile(pathURL.getPath)

//  统计该网站每小时的访问量（pv），该数据每 10 分钟更新一次。
//    val resultDS=sourceDS.map(data => {
//      val arr: Array[String] = data.split(",")
//      var i = 0
//      if (arr(3).equals("pv")){
//        i = 1
//      }
//        (arr(4).toLong,i)
//    })
//      .assignAscendingTimestamps(_._1 * 1000)
//      .timeWindowAll(Time.hours(1),Time.minutes(10))
//      .sum(1)
//    //指定结果位置
//    resultDS.print()
    //启动程序
//    env.execute("Count")
//     统计每小时各商品的用户点击量（pv），该数据每 10 分钟更新一次。
    val resultDS2=sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      var i = 0
      if (arr(3).equals("pv")){
        i = 1
      }
      (arr(1).toInt,arr(4).toLong,i)
    })
      .assignAscendingTimestamps(_._2 * 1000)
      .keyBy(0)
      .timeWindowAll(Time.hours(1),Time.minutes(10))
      .sum(2)
    resultDS2.print()


    env.execute("Count")
  }
}
