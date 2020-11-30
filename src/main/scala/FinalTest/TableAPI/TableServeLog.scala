package FinalTest.TableAPI

import java.net.URL
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import FinalTest.DataStream.ServerLog.serverLog
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table, Tumble}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object TableServeLog {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    val pathURL: URL = getClass.getResource("/FinalTest/apache.log")
    val sourceDS = env.readTextFile(pathURL.getPath).map(data => {
      val arr = data.split(" ")
      val dfs = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val dt = dfs.parse(arr(3))
      //     arr(0):ip,arr(3):timestamp,arr(4):timezone,arr(5):way,arr(6):url
      serverLog(arr(0),dt.getTime,arr(4),arr(5),arr(6))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[serverLog](Time.seconds(40)) {
      override def extractTimestamp(element: serverLog): Long = element.timestamp
    })

    val sourceTable:Table = tableEnv.fromDataStream( sourceDS, 'ip,'timestamp.rowtime,'timezone,'way,'url)
    val table = sourceTable
      .window(Slide over 1.hour every 10.minutes on 'timestamp as 'w)
      .groupBy('url,'w)
      .select('url, 'w.`end` as 'windowEnd, 'url.count as 'cnt)

    tableEnv.createTemporaryView("aggTableView",table,'url,'windowEnd,'cnt)
    var query:String=
      """
        |SELECT *
        |FROM(
        |	SELECT *,
        |			ROW_NUMBER()
        |			 OVER(PARTITION BY windowEnd ORDER BY cnt DESC)
        |			 AS row_num
        |	FROM aggTableView)
        |WHERE row_num<=5
        |""".stripMargin
    val resultTable: Table = tableEnv.sqlQuery(query)
    resultTable.toRetractStream[Row]
      .filter(_._1==true).flatMap(new flatMapFunc2).print()
    env.execute("table API job")
  }
  class flatMapFunc2 extends FlatMapFunction[(Boolean,Row),String]{
    val collect:mutable.Map[(LocalDateTime,Long),(String,LocalDateTime,Long,Long)]=mutable.Map[(LocalDateTime,Long),(String,LocalDateTime,Long,Long)]()
    var oldTime:LocalDateTime = LocalDateTime.MIN
    override def flatMap(data: (Boolean,Row), out: Collector[String]): Unit = {
      val time: LocalDateTime = data._2.getField(1).asInstanceOf[LocalDateTime]
      val url:String= data._2.getField(0).asInstanceOf[String]
      val count:Long = data._2.getField(2).asInstanceOf[Long]
      val serial:Long = data._2.getField(3).asInstanceOf[Long]
      if(oldTime.equals(time)||oldTime.equals(LocalDateTime.MIN)){
        collect += ((time,serial) -> (url,time,count,serial))
      }else{
        val builder: StringBuilder = new StringBuilder
        val strTime: String = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))
        builder.append("窗口的结束时间："+strTime+"\n")
        for(i <- collect.toList.sortBy(_._2._4).iterator){
          builder.append("Top").append(i._2._4).append("\t")
            .append("url="+i._2._1+"\t")
            .append("访问次数="+i._2._3+"\n")
        }
        builder.append("\n====================\n")
        out.collect(builder.toString())
        collect.clear()
      }
      oldTime = time
    }
  }
}
