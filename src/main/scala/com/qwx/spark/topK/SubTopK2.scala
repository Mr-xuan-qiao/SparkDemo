package com.qwx.spark.topK

import java.net.URL

import com.qwx.spark.MySparkUtil
import com.qwx.spark.topK.TopK.topK
import org.apache.spark.rdd.RDD

/**
  * 分组-分学科的topK
  */
object SubTopK2 {
  def main(args: Array[String]): Unit = {
    //数据格式：http://bigdata.edu360.cn/laozhang
    val sc = MySparkUtil(this.getClass.getSimpleName)
    val file: RDD[String] = sc.textFile("D:\\zc.txt")

    val rdd: RDD[((String, String), Int)] = file.map(str => {
      val index: Int = str.lastIndexOf("/")
      val tName: String = str.substring(index + 1)
      val url = new URL(str.substring(0, index))
      val subject: String = url.getHost.split("\\.")(0)

      //封装成一个元组
      ((subject, tName), 1)
    })

    val subNames: Array[String] = rdd.map(_._1._1).distinct().collect

    /*subNames.foreach(subName => {
      //过滤出bigdata这个学科的数据
      val bigSub: RDD[((String, String), Int)] = rdd.filter(t=>subName.equals(t._1._1))
      //wordcount
      val res: RDD[((String, String), Int)] = bigSub.reduceByKey(_+_)
      //按访问次数排序
      val finalRes: RDD[((String, String), Int)] = res.sortBy(_._2)

      finalRes.foreach(println)
    })*/

    for (subName <- subNames){
      //过滤出bigdata这个学科的数据
      val bigSub: RDD[((String, String), Int)] = rdd.filter(t=>subName.equals(t._1._1))
      //wordcount
      val res: RDD[((String, String), Int)] = bigSub.reduceByKey(_+_)
      //按访问次数排序
      val finalRes: RDD[((String, String), Int)] = res.sortBy(_._2)

      finalRes.foreach(println)
    }

    sc.stop()
  }
}
