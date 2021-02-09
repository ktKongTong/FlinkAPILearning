import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

import java.net.URL

object AdClick {
//  用户 Id、广告 Id、用户所在省份、城市、时间戳
//  请按省份统计每小时广告的点击量，该数据每 10 分钟更新一次。
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val pathURL: URL = getClass.getResource("/AdClickLog.csv")
    val sourceDS = env.readTextFile(pathURL.getPath)
    val resultDS=sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(2),arr(4).toLong,1)
    })
      .assignAscendingTimestamps(_._2 * 1000)
      .keyBy(0)
      .timeWindow(Time.hours(1),Time.minutes(10))
      .sum(2)
    resultDS.print()
    env.execute("CountAdClick")
  }
}
