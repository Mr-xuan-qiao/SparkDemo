package com.qwx.spark.sort

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

object MyDualSort {
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


  //按照价格的降序   数量的升序  两个条件排序    思路：利用元组封装排序的条件
  val sortedRDD: RDD[(String, Double, Int)] = formatData.sortBy(t=>(-t._2,t._3))

  sortedRDD.coalesce(1).foreach(println)

  sc.stop()
}
