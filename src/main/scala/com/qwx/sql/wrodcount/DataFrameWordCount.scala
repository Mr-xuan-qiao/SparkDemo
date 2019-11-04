package com.qwx.sql.wrodcount

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val file: RDD[String] = sc.textFile("wc.txt")

    val wordsRDD: RDD[Word] = file.flatMap(_.split(" ")).map((Word(_)))
    val wdf: DataFrame = wordsRDD.toDF()

    //SQL风格
    wdf.registerTempTable("t_words")
    val sql: DataFrame = sqlContext.sql("select word,count(*) as cnt from t_words group by word order by cnt desc")
    //sql.show()

    //DSL风格
    val rowRDD: RDD[Row] = file.flatMap(_.split(" ")).map(Row(_))
    val structType = StructType(List(StructField("word",StringType,true)))
    val dslFrame: DataFrame = sqlContext.createDataFrame(rowRDD,structType)
    dslFrame.select("word").groupBy("word").count().orderBy("count").show()

    sc.stop()
  }
}
case class Word(word:String)