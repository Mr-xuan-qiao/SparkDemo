package com.qwx.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过自定义schema + RDD[Row] 实现DataFrame
  */
object DataFrameDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rdd1: RDD[String] = sc.textFile("person.txt")
    val rowRDD: RDD[Row] = rdd1.map(t => {
      val strs: Array[String] = t.split(" ")
      Row(strs(0), strs(1).toInt, strs(2).toDouble)
    })

    val structType = StructType(List(
      StructField("name", StringType, true),
      StructField("age", IntegerType, false),
      StructField("sal", DoubleType, false)
    ))

    val pdf: DataFrame = sqlContext.createDataFrame(rowRDD,structType)
    pdf.printSchema()
    pdf.select("name").show()

    sc.stop()
  }
}
