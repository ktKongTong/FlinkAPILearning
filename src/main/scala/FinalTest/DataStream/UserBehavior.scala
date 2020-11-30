package FinalTest.DataStream

import java.net.URL

import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction, RichReduceFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
object UserBehavior {

  //用户 Id、商品 Id、商品类别 Id、用户行为和时间戳。
  case class Behavior(userId: Int, commodityId: Int, typeId: Int, behavior: String, timestamp: Long)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val pathURL: URL = getClass.getResource("/FinalTest/UserBehavior.csv")
    val sourceDS: DataStream[Behavior] = env.readTextFile(pathURL.getPath).map(data => {
      val arr = data.split(",")
      Behavior(arr(0).toInt,arr(1).toInt,arr(2).toInt,arr(3),arr(4).toLong)
    })


//    要求1
    val  resultDS1:DataStream[Int]= sourceDS
      .filter(_.behavior=="pv")
      .map((_,1))
      .assignAscendingTimestamps(_._1.timestamp*1000)
      .timeWindowAll(Time.hours(3),Time.hours(1))
      .sum(1)
      .map(_._2)
//      resultDS1.print("pv")

//  要求2
    val  resultDS2:DataStream[Int]= sourceDS
      .filter(_.behavior=="pv")
      .assignAscendingTimestamps(_.timestamp*1000)
      .timeWindowAll(Time.hours(3),Time.hours(1))
      .process( new ProcessFunc1)
//      resultDS2.print("uv")

    //  要求3
    val  resultDS3= sourceDS
      .filter(_.behavior=="pv")
      .assignAscendingTimestamps(_.timestamp*1000)
      .timeWindowAll(Time.hours(1),Time.minutes(10))
      .process(new ProcessFunc2)
    resultDS3.print("item pv")


    env.execute("test")
  }
  class ProcessFunc1 extends ProcessAllWindowFunction[Behavior,Int,TimeWindow]{
    override def process(context: Context, elements: Iterable[Behavior], out: Collector[Int]): Unit = {
      val s:mutable.Set[Int] =  mutable.Set()
      var count:Int = 0
      for (elem <- elements) {
        if(!s.contains(elem.userId)){
          s.add(elem.userId)
          count = count + 1
        }
      }
      out.collect(count)
    }
  }
  class ProcessFunc2 extends ProcessAllWindowFunction[Behavior,(Int,Int),TimeWindow]{
    override def process(context: Context, elements: Iterable[Behavior], out: Collector[(Int,Int)]): Unit = {
      // key为商品id，value为点击量
      val m :mutable.Map[Int,Int] = mutable.Map()
//      对于迭代器中的每一个元素，如果在map中已经存在，就找到该元素，count+1，不存在则添加
      for (elem <- elements){
        if(!m.contains(elem.commodityId)) {
          m += (elem.commodityId -> 1)
        }else{
          val count:Int = m(elem.commodityId)+1
          m += (elem.commodityId -> count)
        }
      }
  //      遍历完所有元素则转为list并按照count个数排序，收集器收count前五条
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
