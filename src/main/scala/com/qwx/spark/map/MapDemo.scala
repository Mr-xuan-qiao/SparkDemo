package com.qwx.spark.map

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

object MapDemo {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

    rdd1.map(_*10)

    //每次迭代一个分区
    rdd1.mapPartitions(it=> it.map(_*10))

    val f = (i:Int,it:Iterator[Int]) => {
      it.map(t=> s"v=$t,p=$i")
    }

    //看数据在哪一个分区
    val index = rdd1.mapPartitionsWithIndex(f)
    val collect: Array[String] = index.collect()
    println(collect.toBuffer)

    sc.stop()
  }
}
