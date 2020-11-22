package StateManager

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CountStateDemo {
  def main(args: Array[String]): Unit = {
    // 1. 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. 获取数据源
    val pathURL: URL = getClass.getResource("/StateManager/sensordata.txt")
    val sourceDS: DataStream[String] = env.readTextFile(pathURL.getPath)
    // 3. 处理数据
    val resultDS: DataStream[(String, Double, Double)] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      (arr(0), arr(1).toDouble, arr(2).toLong)
    })
      .assignAscendingTimestamps(_._3 * 1000)
      .keyBy(_._1)
      .flatMap(new MyStateFlatMapFunc(10))
    // 4. 指定结果位置
    resultDS.print("warning")
    // 5. 启动执行
    env.execute("state job")

  }
  class MyStateFlatMapFunc(threshold: Int) extends RichFlatMapFunction[(String,Double,Long),(String,Double,Double)] {
    var lastTempState: ValueState[Double] = _
    override def open(parameters: Configuration): Unit = {
      lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    }
    override def flatMap(in: (String, Double, Long), collector: Collector[(String, Double, Double)]): Unit = {
      val lastTemp: Double = lastTempState.value()
      val curTemp: Double = in._2
//      不对首个数据进行处理
      if ((lastTemp-curTemp).abs>threshold&&lastTempState.value()!=null){
        val strTime: String = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(in._3*1000))
        collector.collect((strTime+"--"+in._1+"的温度连续变化超过了"+threshold+"°",lastTemp,curTemp))
      }
      lastTempState.update(curTemp)
    }
  }
}