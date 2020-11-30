package FinalTest

import java.sql.Date
import java.text.SimpleDateFormat
import java.util

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//定义输入数据样例类
//543462,1715,1464116,pv,1511658000
case class UBehavior(userId:Long,itemId:Long,categoryId:Long,behavior:String,timestamp:Long)
//定义窗口聚合结果样例类
case class ItemViewCount(itemId:Long,windowEnd:Long,count:Long)

object HotItemsAnalysis {
  def main(args: Array[String]): Unit = {
    // 获取流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 获取数据源，转换为样例类，并提取时间戳
    val sourceDS: DataStream[UBehavior] = env.readTextFile("E:\\code\\IDEAProjects\\FlinkAPILearning\\src\\main\\resources\\FinalTest\\UserBehavior.csv")
      .map(data => {
        val arr: Array[String] = data.split(",")
        UBehavior(arr(0).toLong, arr(1).toLong, arr(2).toLong, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)

    // 得到窗口聚合结果
    val aggDS: DataStream[ItemViewCount] = sourceDS.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(10))
      .aggregate(new AggFunc(), new ItemCountWindowFunc())

    // 按窗口分组，收集当前窗口的商品count数据
    val resultDS: DataStream[String] = aggDS.keyBy(_.windowEnd)
      .process(new TopNItemFunc(5))

    //指定结果的输出方式
    resultDS.print()

    //启动Flink程序
    env.execute("HotItems job")
  }
}

class TopNItemFunc(topSize: Int) extends KeyedProcessFunction[Long,ItemViewCount,String] {
  var itemViewCountListState: ListState[ItemViewCount] = _
  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemcount_list", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    itemViewCountListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val itemViewCounts: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]
    val iter: util.Iterator[ItemViewCount] = itemViewCountListState.get().iterator()
    while (iter.hasNext){
      itemViewCounts += iter.next()
    }
    itemViewCountListState.clear()
    val topNList: ListBuffer[ItemViewCount] = itemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val strTime: String = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(timestamp))
    val builder: StringBuilder = new StringBuilder

    builder.append("窗口的结束时间："+strTime+"\n")

    for (i <- topNList.indices) {
      val currentItemViewCount: ItemViewCount = topNList(i)

      builder.append("Top").append(i+1).append("\t")
        .append("商品ID="+currentItemViewCount.itemId+"\t")
        .append("热门度="+currentItemViewCount.count+"\n")
    }
    builder.append("\n====================\n")
    out.collect(builder.toString())
    Thread.sleep(1000)
  }
}

class ItemCountWindowFunc() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId,windowEnd,count))
  }
}

class AggFunc() extends AggregateFunction[UBehavior,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UBehavior, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}
