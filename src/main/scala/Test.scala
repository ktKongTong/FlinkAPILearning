import scala.collection.immutable.ListMap
import scala.collection.mutable

object Test {
  def main(args: Array[String]): Unit = {
    val elements = List((2,1),(3,1),(4,3))
//    var m: mutable.ListMap[Int, Int] = mutable.ListMap()
//    for (elem <- elements){
//      if(!m.contains(elem)) {
//        m += (elem -> 1)
//      }else{
//        var count= m(elem)+1
//        m += (elem -> count)
//      }
//    }
//    val im = m.toList.sortBy(_._2)
//    println(im)
    if(elements.contains((2,1))){
      println("yes")
    }
  }
}
