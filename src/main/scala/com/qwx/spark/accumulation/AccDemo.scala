package com.qwx.spark.accumulation

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object AccDemo {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)

    //计数器
    var i = 0
    //累加器
    val acc: LongAccumulator = sc.longAccumulator("acc")

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,9),2)
    rdd.foreach(t => {
      i+=1
      acc.add(1)
    })
    println(rdd.count())
    //结果是0，因为i是java变量，在driver端，rdd.foreach是在executor端，所以为0
    println(i)

    println(acc.value)

    sc.stop()
  }
}
