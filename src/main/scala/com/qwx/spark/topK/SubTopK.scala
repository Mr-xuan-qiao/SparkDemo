package com.qwx.spark.topK

import java.net.URL

import com.qwx.spark.MySparkUtil
import com.qwx.spark.topK.TopK.topK
import org.apache.spark.rdd.RDD

/**
  * 分学科的topK
  */
object SubTopK {
  val topK = 3
  def main(args: Array[String]): Unit = {
    //数据格式：http://bigdata.edu360.cn/laozhang
    val sc = MySparkUtil(this.getClass.getSimpleName)
    val file: RDD[String] = sc.textFile("D:\\zc.txt")

    //可用foreach测试
    file.foreach(str=>{
      val index: Int = str.lastIndexOf("/")
      val tName: String = str.substring(index+1)
      val url = new URL(str.substring(0,index))
      val subject: String = url.getHost.split("\\.")(0)
      //println(subject)
    })

    val rdd: RDD[((String, String), Int)] = file.map(str => {
      val index: Int = str.lastIndexOf("/")
      val tName: String = str.substring(index + 1)
      val url = new URL(str.substring(0, index))
      val subject: String = url.getHost.split("\\.")(0)

      //封装成一个元组
      ((subject, tName), 1)
    })
    //按key处理
    val result: RDD[((String, String), Int)] = rdd.reduceByKey(_+_)

    //处理过的累加数据进行按学科分组
    val groupedRes: RDD[(String, Iterable[((String, String), Int)])] = result.groupBy(it => {
      it._1._1
    })

    //对每一个学科对应的  迭代器进行 排序
    val finalRes: RDD[(String, List[(String, Int)])] = groupedRes.mapValues(it => {
      //Iterator没有sortBy函数，将Iterator转为List，整理数据格式
      //使用treeMap/treeSet   设置排序的规则是  按照次数降序 设置长度为topK
      it.toList.sortBy(-_._2).map(it => (it._1._2, it._2)).take(topK)
    })

    finalRes.foreach(println)

    sc.stop()
  }
}
