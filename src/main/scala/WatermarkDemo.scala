import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.sys.env

object WatermarkDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceDS: DataStream[String] = env.socketTextStream("127.0.0.1", 8887)

    val dataDStream:(DataStream[(String, Int, Long)]) = sourceDS.map(record => {
      val arr = record.split(",")
      (arr(0),arr(1).toInt,arr(2).toLong)
    })

    // 抽取timestamp 和 watermark

    val waterMarkStream = dataDStream.assignTimestampsAndWatermarks(StreamingPeriodicWatermark)

    // 保存被丢弃的乱序数据
    val lateOutputTag = OutputTag[(String, Int, Long)]("late-data")
    val window = waterMarkStream
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 按照消息的EventTime分配窗口，和调用TimeWindow效果一样
      .allowedLateness(Time.seconds(4)) // 允许延迟2s
      .sideOutputLateData(lateOutputTag)
      .maxBy(1)

      window.print()

    // 把延迟的数据暂时打印到控制台，实际可以保存到存储介质中。
    val sideOutput = window.getSideOutput(lateOutputTag)
    sideOutput.print()

    env.execute("WatermarkDemo")
  }
}
object StreamingPeriodicWatermark extends AssignerWithPeriodicWatermarks[(String, Int, Long)]{

  var currentMaxTimestamp = 0L
  val maxOutOfOrderness = 2000L // 最大允许的乱序时间是10s

  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  override def extractTimestamp(element: (String, Int, Long), previousElementTimestamp: Long): Long = {
    val timestamp = element._3*1000
    currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp)
    timestamp
  }

}
