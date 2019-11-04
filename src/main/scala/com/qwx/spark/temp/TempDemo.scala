package com.qwx.spark.temp

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

object TempDemo {
  def main(args: Array[String]): Unit = {
    val d1 = Array(("bj",28.1),("sh",28.7),("gz",32.0),("sz",33.1))
    val d2 = Array(("bj",27.3),("sh",30.1),("gz",33.3))
    val d3 = Array(("bj",28.2),("sh",29.1),("gz",32.0),("sz",30.5))

    val sc = MySparkUtil(getClass.getSimpleName)

    var data = d1 ++ d2 ++ d3
    val rdd1: RDD[(String, Double)] = sc.makeRDD(data)

    //groupByKey实现
    val groupedRdd: RDD[(String, Iterable[Double])] = rdd1.groupByKey()
    val groupedResult: RDD[(String, Double)] = groupedRdd.mapValues(it=> it.sum / it.size)
    groupedResult.foreach(println)

    //reduceByKey实现
    val rdd2: RDD[(String, List[Double])] = rdd1.mapValues(List(_))
    val reduceRdd: RDD[(String, List[Double])] = rdd2.reduceByKey(_++_)
    val reducedRes: RDD[(String, Double)] = reduceRdd.mapValues(t=>(t.sum / t.size))
    reducedRes.foreach(println)

    //groupBy实现
    val groupByed: RDD[(String, Iterable[(String, Double)])] = rdd1.groupBy(_._1)
    val groupByRes: RDD[(String, Double)] = groupByed.mapValues(t=>t.map(_._2).sum / t.size)
    groupByRes.foreach(println)

    sc.stop()
  }
}
