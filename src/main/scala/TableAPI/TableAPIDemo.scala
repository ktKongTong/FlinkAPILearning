package TableAPI

import java.net.URL

import StateManager.CountStateDemo.getClass
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.api.scala._

object TableAPIDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)
    val inputPathURL: URL = getClass.getResource("/TableAPI/sensordata.csv")
    tableEnv.connect(new FileSystem()
      .path(inputPathURL.getPath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temperature",DataTypes.DOUBLE())
        .field("timestamp",DataTypes.BIGINT()))
      .createTemporaryTable("inputTable")

    val sinkDDL: String =
      """
        |create table jdbcOutputTable (
        |	id varchar(50) ,
        |	cnt bigint
        |) with (
        |	'connector.type' = 'jdbc',
        |	'connector.url' = 'jdbc:mysql://127.0.0.1:3306/testdb',
        |	'connector.table' = 'sensor_count',
        |	'connector.driver' = 'com.mysql.cj.jdbc.Driver',
        |	'connector.username' = 'root',
        |	'connector.password' = 'root'
        |)  """.stripMargin

    tableEnv.sqlUpdate(sinkDDL)	// 执行 DDL创建表

    val sourceTable: Table = tableEnv.from("inputTable")

    //table API分组查询-打印输出
    sourceTable.groupBy('id)
      .select('id,'id.count)
      .toRetractStream[(String,Long)]
      .print("api result")

    //table API分组查询-写入MySQL（支持）
    sourceTable.groupBy('id)
      .select('id,'id.count)
      .insertInto("jdbcOutputTable")


//    val groupQuery:String=
//      """
//        |select id,count(*)
//        |from inputTable
//        |group by id
//        |""".stripMargin
//
//    // 执行FlinkSQL,打印输出
//    tableEnv.sqlQuery(groupQuery)
//      .toRetractStream[(String,Long)]
//      .print("sql result")
//
//    // 执行FlinkSQL,写入Mysql
//    tableEnv.sqlQuery(groupQuery)
//      .insertInto("jdbcOutputTable")


    env.execute("flink SQL job")
  }
}
