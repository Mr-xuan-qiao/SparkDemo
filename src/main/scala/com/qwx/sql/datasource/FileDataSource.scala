package com.qwx.sql.datasource

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object FileDataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    //同一个类型的文件一起读
    val text: Dataset[String] = spark.read.textFile("person.txt","wc.txt")
    val text1: DataFrame = spark.read.text("wc.txt")

    val file: Dataset[String] = spark.read.textFile("person.txt")
    val df2: Dataset[(String, String, String)] = file.map(str => {
      val split: Array[String] = str.split(" ")
      (split(0), split(1), split(2))
    })

    //text API只支持单列，String类型    //报错
    //df2.write.text("output1")
    //save API 数据格式是parquet
    file.write.save("output2")
    //将dataSet  转为  RDD  再写入文件
    df2.rdd.saveAsTextFile("output3")

    spark.stop()
  }
}
