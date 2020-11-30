package FinalTest.DataStream

import java.net.URL
import java.text.SimpleDateFormat

import FinalTest.DataStream.UserBehavior.{Behavior, getClass}
import akka.util.Helpers.timestamp
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

object ServerLog {
  //    IP,Id(屏蔽),name(屏蔽),timestamp,timezone,way,url
  case class serverLog(ip: String, timestamp: Long, timezone: String, way: String, url: String)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val pathURL: URL = getClass.getResource("/FinalTest/apache.log")
    val sourceDS: DataStream[serverLog] = env.readTextFile(pathURL.getPath).map(data => {
      val arr = data.split(" ")
      val time = arr(3)
      val dfs = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val dt = dfs.parse(time)
//     arr(0):ip,arr(3):time,arr(4):timezone,arr(5):way,arr(6):url
      serverLog(arr(0),dt.getTime,arr(4),arr(5),arr(6))
    })
    val resultDS= sourceDS
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[serverLog](Time.milliseconds(0)) {
      override def extractTimestamp(element: serverLog): Long = element.timestamp
    })
      .timeWindowAll(Time.hours(1),Time.minutes(10))
      .process(new ProcessFunc)
    resultDS.print()
    env.execute("2")
  }

  class ProcessFunc extends ProcessAllWindowFunction[serverLog,(String,Int),TimeWindow]{
    override def process(context: Context, elements: Iterable[serverLog], out: Collector[(String,Int)]): Unit = {
      val m :mutable.Map[String,Int] = mutable.Map()
      for (elem <- elements){
        if(!m.contains(elem.url)) {
          m += (elem.url -> 1)
        }else{
          val count:Int = m(elem.url)+1
          m += (elem.url -> count)
        }
      }
      val imList = m.toList.sortBy(_._2)
      if(imList.length>=5){
        for(i <- 1 to 5){
          out.collect(imList(imList.length-i))
        }
      }else{
        for (i <- imList.iterator){
          out.collect(i)
        }
      }
      println("="*15)
    }
  }
}
