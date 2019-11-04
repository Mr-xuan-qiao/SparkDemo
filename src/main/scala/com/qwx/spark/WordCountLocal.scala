package com.qwx.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCountLocal {
  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("Usage:com.qwx.spark.WordCount <input><output>")
      sys.exit(1)
    }

    val Array(input,output)= args

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    //SparkContext
    val sc:SparkContext = new SparkContext(conf)
    //sc.textFile("").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2).saveAsTextFile("")
    //读取文件
    val rdd1: RDD[String] = sc.textFile(input)
    //切分并压平
    val wordsrdd: RDD[String] = rdd1.flatMap(_.split(" "))
    //组装
    val wordAndOne: RDD[(String,Int)] = wordsrdd.map((_,1))
    //分组聚合
    val result: RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)
    //排序 制定降序
    val sort = result.sortBy(-_._2)

    println(sort.toDebugString)
    //存储
    sort.saveAsTextFile(output)

    //释放资源
    sc.stop()
  }

}
