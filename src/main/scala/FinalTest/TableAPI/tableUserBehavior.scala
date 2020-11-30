package FinalTest.TableAPI

import java.net.URL

import FinalTest.DataStream.UserBehavior.Behavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table, Tumble}
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._

object tableUserBehavior {
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
      .assignAscendingTimestamps(_.timestamp*1000)
    val sourceTable:Table = tableEnv.fromDataStream(sourceDS,'userId,'commodityId,'typeId,'behavior,'timestamp.rowtime)
    val table= sourceTable
      .window(Slide over 1.hour every 10.minutes on 'timestamp as 'w)
      .groupBy('commodityId,'w)
      .select('commodityId, 'w.`end` as 'windowEnd, 'commodityId.count as 'cnt)
    tableEnv.createTemporaryView("aggTableView",table,'commodityId,'windowEnd,'cnt)
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
    resultTable.toRetractStream[Row].print()
    env.execute("test")
  }
}
