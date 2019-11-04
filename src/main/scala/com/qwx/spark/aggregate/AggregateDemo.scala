package com.qwx.spark.aggregate

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)

    val rdd1 = sc.makeRDD(List(List(1,3),List(2,4),List(3,5)),3)

    //a:累加值(zeroValue)     b:元素值
    //求各元素中最大值的和
    val res1: Int = rdd1.aggregate(0)((a,b)=>a+b.max,((a,b)=>a+b))
    //求各元素中每个元素的和
    val res2: Int = rdd1.aggregate(0)((a,b)=>a+b.sum,((a,b)=>a+b))
    val res3: Int = rdd1.aggregate(0)(_+_.sum,_+_)

    val pairRDD = sc.parallelize(List(("cat",2),("cat",5),("mouse",4),("cat",12),("dog",12),("mouse",12)),2)
    val pairRes: RDD[(String, Int)] = pairRDD.aggregateByKey(0)(_+_,_+_)

    val collect: Array[(String, Int)] = pairRes.collect
    println(collect.foreach(println))
  }
}
