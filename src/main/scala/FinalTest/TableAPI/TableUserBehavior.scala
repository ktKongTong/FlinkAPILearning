package FinalTest.TableAPI

import java.net.URL
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import FinalTest.DataStream.UserBehavior.Behavior
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table, Tumble}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable

object TableUserBehavior {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    val pathURL: URL = getClass.getResource("/FinalTest/UserBehavior.csv")
    val sourceDS: DataStream[Behavior] = env.readTextFile(pathURL.getPath).map(data => {
      val arr = data.split(",")
      Behavior(arr(0).toInt,arr(1).toInt,arr(2).toInt,arr(3),arr(4).toLong)
    }).filter(_.behavior=="pv")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Behavior](Time.minutes(2)) {
        override def extractTimestamp(element: Behavior): Long = {
          element.timestamp*1000+3600*16*1000
        }
      })
    val sourceTable:Table = tableEnv.fromDataStream(sourceDS,'userId,'commodityId,'typeId,'behavior,'timestamp.rowtime)

    //要求1
    val table1= sourceTable
      .window(Slide over 3.hour every 1.hour on 'timestamp as 'w)
      .groupBy('w)
      .select('w.`end` as 'windowEnd, 'behavior.count as 'cnt)
//      table1
//        .toRetractStream[Row]
//        .print()

    //要求2
    val table2= sourceTable
      .window(Slide over 3.hour every 1.hour on 'timestamp as 'w)
      .groupBy('w)
      .select('userId.count.distinct,'w.`end` as 'windowEnd)
//    table2
//      .toRetractStream[Row]
//      .print()



    //要求3
    val table3= sourceTable
      .window(Slide over 1.hour every 10.minutes on 'timestamp as 'w)
      .groupBy('commodityId,'w)
      .select('commodityId, 'w.`end` as 'windowEnd, 'behavior.count as 'cnt)

    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        |	commodityId bigint ,
        | windowEnd timestamp(3) ,
        |	cnt bigint,
        | row_num bigint not null
        |) with (
        |	'connector.type' = 'jdbc',
        |	'connector.url' = 'jdbc:mysql://localhost:3306/testdb',
        |	'connector.table' = 'hotItem',
        |	'connector.driver' = 'com.mysql.cj.jdbc.Driver',
        |	'connector.username' = 'root',
        |	'connector.password' = 'Hadoop2!'
        |)  """.stripMargin

    tableEnv.sqlUpdate(sinkDDL)


    tableEnv.createTemporaryView("aggTableView3",table3,'commodityId,'windowEnd,'cnt)
    var query3:String=
      """
        |SELECT *
        |FROM(
        |	SELECT *,
        |			ROW_NUMBER()
        |			 OVER(PARTITION BY windowEnd ORDER BY cnt DESC)
        |			 AS row_num
        |	FROM aggTableView3)
        |WHERE row_num<=5
        |""".stripMargin
     tableEnv.sqlQuery(query3).insertInto("jdbcOutputTable")
//    resultTable3.toRetractStream[Row]
//      .filter(_._1==true).flatMap(new flatMapFunc).print()
    env.execute("test")
  }

  class flatMapFunc extends FlatMapFunction[(Boolean,Row),String]{
    val collect:mutable.Map[(LocalDateTime,Long),(Int,LocalDateTime,Long,Long)]=mutable.Map[(LocalDateTime,Long),(Int,LocalDateTime,Long,Long)]()
    var oldTime:LocalDateTime = LocalDateTime.MIN
    override def flatMap(data: (Boolean,Row), out: Collector[String]): Unit = {
      val time: LocalDateTime = data._2.getField(1).asInstanceOf[LocalDateTime]
      val itemId:Int= data._2.getField(0).asInstanceOf[Int]
      val count:Long = data._2.getField(2).asInstanceOf[Long]
      val serial:Long = data._2.getField(3).asInstanceOf[Long]
      if(oldTime.equals(time)||oldTime.equals(LocalDateTime.MIN)){
        collect += ((time,serial) -> (itemId,time,count,serial))
      }else{
        val builder: StringBuilder = new StringBuilder
        val strTime: String = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))
        builder.append("窗口的结束时间："+strTime+"\n")
        for(i <- collect.toList.sortBy(_._2._4).iterator){
          builder.append("Top").append(i._2._4).append("\t")
            .append("商品ID="+i._2._1+"\t")
            .append("热门度="+i._2._3+"\n")
        }
        builder.append("\n====================\n")
        out.collect(builder.toString())
        collect.clear()
      }
      oldTime = time
    }
  }
}
