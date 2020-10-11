import org.apache.flink.api.common.functions.{AggregateFunction, IterationRuntimeContext, ReduceFunction, RichFunction, RichMapFunction, RuntimeContext}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object EventTimeWindowDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val sourceDS: DataStream[String] = env.socketTextStream("127.0.0.1", 8888)

    //统计每30s顾客消费的最大值(要求 1 )
    val resultDS1: DataStream[(Int, Int, Int, Float, Long)] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toFloat, arr(4).toLong)
    })
      .assignAscendingTimestamps(_._5*1000)
      .keyBy(1)
      .timeWindow(Time.seconds(30))
      .maxBy(3)
    resultDS1.print()

    //按顾客划分30s内的数据，每10s划分一次
    val windowedStream2: WindowedStream[(Int, Int, Int, Float, Long), Tuple, TimeWindow] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toFloat, arr(4).toLong)
    })
      .assignAscendingTimestamps(_._5*1000)
      .keyBy(1)
      .timeWindow(Time.seconds(30), Time.seconds(10))
    //统计上述数据每个顾客的消费总金额以及订单数量(要求 2 )
    windowedStream2.aggregate(new AggregateFunction[(Int, Int, Int, Float, Long), (Int, Float), (Int, Float)] {
      // 迭代的初始值
      override def createAccumulator(): (Int, Float) = (0, 0)
      // 每一个数据如何和迭代数据 迭代
      override def add(value: (Int, Int, Int, Float, Long), accumulator: (Int, Float)): (Int, Float) = {
        (accumulator._1+1,accumulator._2 + value._4)
      }
      // 返回结果
      override def getResult(accumulator: (Int, Float)): (Int, Float) = accumulator
      // 每个分区数据之间如何合并数据
      override def merge(acc: (Int, Float), acc1: (Int, Float)): (Int, Float) = {
        (acc._1+acc1._1,acc._2+acc1._2)
      }
    }).print("s")

    //按商品类型划分，每10s统计一次30s内的数据
    val windowedStream3: WindowedStream[(Int, Int, Int, Float, Long), Tuple, TimeWindow] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toFloat, arr(4).toLong)
    })
      .assignAscendingTimestamps(_._5*1000)
      .keyBy(2)
      .timeWindow(Time.seconds(30), Time.seconds(10))


    //统计上述数据每个商品的销售数量(要求 3 )


    env.execute("EventTimeWindowDemo")
  }
}
