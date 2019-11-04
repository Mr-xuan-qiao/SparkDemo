package com.qwx.spark.groupby

import com.qwx.spark.MySparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySparkUtil(getClass.getSimpleName)

    //groupBy   根据指定条件进行分组    key是由自定义函数的返回值类型决定的
    //RDD[K]     RDD[K,V]-
    val rdd1 = sc.makeRDD(List("reba","baby","nazha","dr","nazha"))
    val rdd2 = sc.makeRDD(List(10,20,30))

    val by: RDD[(String, Iterable[String])] = rdd1.groupBy(t => t)
    val by1: RDD[(String, Iterable[Int])] = rdd2.groupBy(t=>t.toString)

    //利用groupBy实现wordcount
    val words: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",1),("a",1),("d",1)))
    val grouped1 = words.groupBy(_._1)
    val result1 = grouped1.mapValues(t=>t.map(_._2).sum)

    //groupByKey  只能按key  RDD[K,V]
    val group2: RDD[(String, Iterable[Int])] = words.groupByKey(3)
    val result2: RDD[(String, Int)] = group2.mapValues(_.sum)

    //reduceByKey  RDD[K,V]
    val result3: RDD[(String, Int)] = words.reduceByKey(_+_)
  }
}
