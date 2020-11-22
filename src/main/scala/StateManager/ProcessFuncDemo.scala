package StateManager

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFuncDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val sourceDS: DataStream[String] = env.socketTextStream("127.0.0.1", 8887)
    val resultDS = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1).toDouble, arr(2).toLong)
    })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Double, Long)](Time.seconds(0)) {
          override def extractTimestamp(t: (String, Double, Long)): Long = {
            t._3 * 1000
          }
        })
      .keyBy(_._1)
      .process(new My(5))

    resultDS.print("warning")
    env.execute("process job")
  }
  class My(interval:Int) extends KeyedProcessFunction[String,(String,Double,Long),String]{
    var lastTempState: ValueState[Double] = _
    var timeState: ValueState[Long] = _
    var countState: ValueState[Int] = _
    override def open(parameters: Configuration): Unit = {
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
      timeState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",classOf[Long]))
      countState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("count",classOf[Int]))
    }
    override def processElement(value: (String, Double, Long),
                                ctx: KeyedProcessFunction[String, (String, Double, Long), String]#Context,
                                out: Collector[String]): Unit = {
      println("="*15)
      println("processElement")
      val preTemp: Double = lastTempState.value()
      val curTemp: Double = value._2
      val count: Int = countState.value()
      val time: Long = timeState.value()
      lastTempState.update(curTemp)
      if(timeState.value() == null){
        //首个数据的处理
        timeState.update(value._3*1000)
      }else if (curTemp>preTemp){
        //温度上升
        countState.update(count+1)
        println("up! it's "+countState.value()+" times")
      }
      else if(curTemp < preTemp){
        //温度下降
        println("down!")
        //删除多余的计时器
        for ( i <- Range(countState.value()+1-interval,countState.value()+1)){
          if(countState.value()<5){

          }
          println("delete timer: "+(time+(i+interval)*1000))
          ctx.timerService().deleteEventTimeTimer(time+(i+interval)*1000)
        }
        timeState.clear()
        countState.clear()
        //更新起始时间
        timeState.update(value._3*1000)
      }
      //为每一条数据都注册计时器，如果下降，就将前面未触发的计时器删除
      ctx.timerService().registerEventTimeTimer((value._3+interval)*1000)
      println("watermark:"+ctx.timerService().currentWatermark()+",timestamp:"+ctx.timestamp())
      println("preTemp:"+preTemp+",curTemp:"+curTemp+", startTime:"+time)
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Double, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      val key = ctx.getCurrentKey
      val strTime: String = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(timeState.value()))
      //对于尾部数据，因为设置了计时器，一旦结束会立即触发，所以添加条件>4
      if (countState.value()>4){
//        out.collect(timestamp+"--"+strTime+key+"的温度连续"+countState.value()+"秒持续上升!")
        println("="*15)
        println("Fire！！！")
        out.collect(timestamp+" ,Fire! "+"since "+timeState.value()+" ("+strTime +") " +key + "'s temperature keep increasing for "+countState.value()+" times")
      }
    }
  }
}