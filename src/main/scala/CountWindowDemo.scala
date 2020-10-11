import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object CountWindowDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceDS: DataStream[String] = env.socketTextStream("47.93.48.239", 12345)
    val resultDS: DataStream[(String, Int)] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1).toInt)
    })
      .keyBy(0)
      //滚动计数窗口
//      .countWindow(5)
      //滑动计数窗口
      .countWindow(5,2)
      .sum(1)
    resultDS.print()
    env.execute("CountWindowDemo")
  }
}
