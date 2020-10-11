import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object ProcessTimeWindowDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val sourceDS: DataStream[String] = env.socketTextStream("47.93.48.239", 12345)
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
