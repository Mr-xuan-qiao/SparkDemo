package com.qwx.spark.topK

import java.net.URL

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

/**
  * 全局topkey
  */
object TopK {
  val topK = 2
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

    val result: RDD[((String, String), Int)] = rdd.reduceByKey(_+_)

    //排序，按照访问次数降序
    val finalRes: Array[((String, String), Int)] = result.sortBy(-_._2).take(topK)

    finalRes.foreach(println)

    sc.stop()
  }
}
