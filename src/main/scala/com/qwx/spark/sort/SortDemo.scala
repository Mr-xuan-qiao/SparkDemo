package com.qwx.spark.sort

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

object SortDemo {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)

    val rdd: RDD[Int] = sc.makeRDD(List(11,2,3,9,1),1)

    //排序，  单独写_不能被解析成函数   +_:升序    -_:降序   可以用 t=>t
    //foreach无法保证顺序，使用coalesce()   函数将分区转为1 个分区，确保打印出来  有序
    //rdd.sortBy(+_).coalesce(1).foreach(println)
    rdd.sortBy(+_).foreach(println)


    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("xy",2000),("jl",100),("fge",10000)))
    //false:降序
    rdd2.map(_.swap).sortByKey(false).coalesce(1).foreach(println)
    rdd2.sortBy(-_._2).coalesce(1).foreach(println)

    rdd2.repartition(2)

    sc.stop()
  }
}
