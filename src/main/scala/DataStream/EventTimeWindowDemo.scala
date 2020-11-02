package DataStream

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object EventTimeWindowDemo {
  /*
  * Question 4
  *
  * */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceDS: DataStream[String] = env.socketTextStream("127.0.0.1", 8888)

    /*
    * 要求 1
    * 每 30 秒钟，统计一次各用户的最大消费订单信息，将结果写入 MySQL
    * */
    val resultDS1: DataStream[(Int, Int, Int, Float, Long)] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toFloat, arr(4).toLong)
    })
      .assignAscendingTimestamps(_._5 * 1000)
      .keyBy(1)
      .timeWindow(Time.seconds(30))
      .maxBy(3)
    resultDS1.print()
    resultDS1.addSink(new MyJdbcSink)
    class MyJdbcSink() extends RichSinkFunction[(Int, Int, Int, Float, Long)] {
      //定义sql连接、预编译器
      var conn: Connection = _
      var insertStmt: PreparedStatement = _
      var updateStmt: PreparedStatement = _

      //初始化 创建连接 和 预编译语句
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
        insertStmt = conn.prepareStatement("insert into orders (orderId,customId,commodityId,price,timestamps) values (?,?,?,?,?)")
        updateStmt = conn.prepareStatement("update orders set customId=?,price=?,timestamps=?, commodityId = ? where orderId = ?")
      }

      // 调用连接 执行sql
      override def invoke(value: (Int, Int, Int, Float, Long), context: SinkFunction.Context[_]): Unit = {
        // 执行更新语句
        updateStmt.setInt(1, value._2)
        updateStmt.setInt(2, value._3)
        updateStmt.setFloat(3, value._4)
        updateStmt.setLong(4, value._5)
        updateStmt.setInt(5, value._1)
        updateStmt.execute()
        //如果update没有更新 即 没有查询到数据 即 没有该id 那么执行插入
        if (updateStmt.getUpdateCount == 0) {
          insertStmt.setInt(1, value._1)
          insertStmt.setInt(2, value._2)
          insertStmt.setInt(3, value._3)
          insertStmt.setFloat(4, value._4)
          insertStmt.setLong(5, value._5)
          insertStmt.execute()
        }
      }

      //关闭时做清理工作
      override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
      }
    }
    /*
    * 要求 2
    * 统计 30 秒内，各用户的消费总额和订单数量，该数据每 10 秒更新一次，将结果打印输出
    * */
    val windowedStream2: WindowedStream[(Int, Int, Int, Float, Long), Tuple, TimeWindow] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toFloat, arr(4).toLong)
    })
      .assignAscendingTimestamps(_._5 * 1000)
      .keyBy(1)
      .timeWindow(Time.seconds(30), Time.seconds(10))
    //统计上述数据每个顾客的消费总金额以及订单数量(要求 2 )
    windowedStream2.aggregate(new AggregateFunction[(Int, Int, Int, Float, Long), (Int, Int, Float), (Int, Int, Float)] {
      // 迭代的初始值
      override def createAccumulator(): (Int, Int, Float) = (0, 0, 0)

      // 每一个数据如何和迭代数据 迭代
      override def add(value: (Int, Int, Int, Float, Long), accumulator: (Int, Int, Float)): (Int, Int, Float) = {
        (value._2, accumulator._2 + 1, accumulator._3 + value._4)
      }

      // 返回结果
      override def getResult(accumulator: (Int, Int, Float)): (Int, Int, Float) = accumulator

      // 每个分区数据之间如何合并数据
      override def merge(acc: (Int, Int, Float), acc1: (Int, Int, Float)): (Int, Int, Float) = {
        (acc._1, acc._2 + acc1._2, acc._3 + acc1._3)
      }
    }).print()


    /*
    * 要求 3
    * 统计 30 秒内，各商品的销售数量，该数据每 10 秒更新一次，将结果打印输出
    * */
    val windowedStream3: WindowedStream[(Int, Int, Int, Float, Long), Tuple, TimeWindow] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toFloat, arr(4).toLong)
    })
      .assignAscendingTimestamps(_._5 * 1000)
      .keyBy(2)
      .timeWindow(Time.seconds(30), Time.seconds(10))
    //统计上述数据每个商品的销售数量(要求 3 )
    windowedStream3.aggregate(new AggregateFunction[(Int, Int, Int, Float, Long), (Int, Int), (Int, Int)] {
      // 迭代的初始值
      override def createAccumulator(): (Int, Int) = (0, 0)

      // 每一个数据如何和迭代数据 迭代
      override def add(value: (Int, Int, Int, Float, Long), accumulator: (Int, Int)): (Int, Int) = {
        (value._3, accumulator._2 + 1)
      }

      // 返回结果
      override def getResult(accumulator: (Int, Int)): (Int, Int) = accumulator

      // 每个分区数据之间如何合并数据
      override def merge(acc: (Int, Int), acc1: (Int, Int)): (Int, Int) = {
        (acc._1, acc._2 + acc1._2)
      }
    }).print()

    env.execute("EventTimeWindowDemo")
  }
}
