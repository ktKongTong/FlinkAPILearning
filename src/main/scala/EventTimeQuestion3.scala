
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeQuestion3 {
  def main(args: Array[String]): Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      val sourceDS: DataStream[String] = env.socketTextStream("127.0.0.1", 8886)
      val resultDS: DataStream[(String, Int, Long)] = sourceDS.map(data => {
        val arr: Array[String] = data.split(",")
        (arr(0), arr(1).toInt, arr(2).toLong)
      })
        .assignAscendingTimestamps(_._3*1000)
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .maxBy(1)
      resultDS.print()
      env.execute("EventTimeQuestion3Demo")
  }
}
