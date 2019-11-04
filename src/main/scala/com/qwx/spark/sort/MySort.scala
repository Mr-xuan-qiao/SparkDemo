package com.qwx.spark.sort

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object MySort {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)

    val strings: List[String] = List[String]("shouji 5999 1000","shoulei 199 3","shoukao 200 10","lazhu 3 1000","feizao 4.5 1000")
    val data: RDD[String] = sc.makeRDD(strings)

    //数据预处理（将String转为对应的类型，方便排序）
    val formatData: RDD[(String, Double, Int)] = data.map(t => {
      val proName: String = t.split(" ")(0)
      val price: Double = t.split(" ")(1).toDouble
      val saleNum: Int = t.split(" ")(2).toInt

      (proName, price, saleNum)
    })

    //单个条件按价格降序排序
    //coalesce(1)  为了使结果打印正确
    val sortedData: RDD[(String, Double, Int)] = formatData.sortBy(-_._2).coalesce(1)
    sortedData.foreach(println)

    //多个条件排序

    sc.stop()
  }
}
