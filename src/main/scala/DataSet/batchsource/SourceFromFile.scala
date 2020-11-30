package DataSet.batchsource

import java.net.URL
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object SourceFromFile {
  case class Student(className: String, stuNo:String,name:String,gender: String, score: Int)
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val pathURL: URL = getClass.getResource("/DataSet/csvdata/student.csv")
    val sourceDS2 = env.readCsvFile[Student](pathURL.getPath)
    println("totalCount:"+sourceDS2.count())
    println("boyCount:"+sourceDS2.filter(_.gender=="男").count())
    println("girlCount:"+sourceDS2.filter(_.gender=="女").count())

    print("boyMinScore:")
    sourceDS2.filter(_.gender=="男").minBy(4).print()
    print("boyMaxScore:")
    sourceDS2.filter(_.gender=="男").maxBy(4).print()
    print("girlMinScore:")
    sourceDS2.filter(_.gender=="女").minBy(4).print()
    print("girlMaxScore:")
    sourceDS2.filter(_.gender=="女").maxBy(4).print()
    print("minScore:")
    sourceDS2.minBy(4).print()
    print("maxScore:")
    sourceDS2.maxBy(4).print()
  }
}
