package com.qwx.sql.datasource

import org.apache.spark.sql.{DataFrame, SparkSession}

object NestJsonDataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    val json2: DataFrame = spark.read.json("log2.json")
    json2.printSchema()
    json2.show()

    ///DSL获得city
    json2.select("province.city").show()
    //SQL方式
    json2.createTempView("json")
    spark.sql("select province.city from json").show()

  }
}
