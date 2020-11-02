package DataStream

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object ProcessTimeWindowDemo {
  /*
  * Question1
  * */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceDS: DataStream[String] = env.socketTextStream("127.0.0.1", 8887)
    val resultDS: DataStream[(String, Int)] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1).toInt)
    })
      .keyBy(0)
      .timeWindow(Time.seconds(15))
      //      .timeWindow(Time.seconds(15),Time.seconds(10))
      .sum(1)
    resultDS.print()
    env.execute("ProcessTimeWindowDemo")
  }
}
