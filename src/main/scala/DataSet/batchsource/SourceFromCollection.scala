package DataSet.batchsource


import java.net.URL

import org.apache.flink.api.common.accumulators.{IntCounter, LongCounter}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.collection.JavaConverters.asScalaBufferConverter

object SourceFromCollection {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

//    筛选
    val ds2 :DataSet[String]= env.fromCollection(List("hadoop", "hdfs", "yarn","flink","spark","hbase"))
    val pathURL1: URL = getClass.getResource("/DataSet/result/result.txt")
    ds2.filter(_.length>4)
      .filter(_.startsWith("h"))
      .writeAsText(pathURL1.getPath,WriteMode.OVERWRITE)


    println("=" * 15)

// 广播变量
    val ds3:DataSet[(Int,String)] = env.fromCollection(List((1,"张三"),(2,"李四"),(3,"王五")))
    val ds4:DataSet[(Int,String,Int)] = env.fromCollection(List((1,"Hadoop大数据技术",90),(1,"大数据处理与编程技术",88),
        (2,"Hadoop大数据技术",76),(3,"Hadoop大数据技术",85),(3,"大数据导论",92)))
    ds4.map(new RichMapFunction[(Int,String,Int),(String,String,Int)] {
      var broadcastSet: Traversable[(Int,String)]=null
      override def open(parameters: Configuration): Unit = {
        broadcastSet=getRuntimeContext.getBroadcastVariable[(Int,String)]("testBroad").asScala
      }
      override def map(in: (Int, String,Int)): (String, String, Int) = {
        val out = broadcastSet.filter(_._1==in._1).map(inA => (inA._2,in._2,in._3)).toList
        out.head
      }
    }).withBroadcastSet(ds3,"testBroad").print()


    println("=" * 15)

//  累加器
    val ds= env.fromElements("hadoop", "hdfs", "yarn","flink","spark","hbase","hive","mapreduce")
    val res=ds.map(new RichMapFunction[String,String] {
      val count = new IntCounter()
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //获取累加器
        getRuntimeContext.addAccumulator("count",count)
      }

      override def map(in: String): String = {
        count.add(1)
        in
      }
    }).setParallelism(4)
    val pathURL: URL = getClass.getResource("/DataSet/result")
    res.writeAsText(pathURL.getPath, WriteMode.OVERWRITE)
    val result = env.execute("SourceFromCollection")
    val num = result.getAccumulatorResult[Int]("count")
    println("num:"+num)
  }
}
