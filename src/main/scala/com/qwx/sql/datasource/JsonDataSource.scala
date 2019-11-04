package com.qwx.sql.datasource

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JsonDataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    //数据格式：{"pname":"meinv", "price":200, "amount":10}
    val jsonDF: DataFrame = spark.read.json("log.json")
    jsonDF.printSchema()
    jsonDF.show()

    //数据格式：{"pname":"meinv", "price":200, "amount":10,"province":{"city":"bj"}}
    val json2: DataFrame = spark.read.json("log2.json")
    json2.printSchema()
    json2.show()
    ///DSL获得city
    json2.select("province.city").show()
    //SQL方式
    json2.createTempView("json")
    spark.sql("select province.city from json").show()
    //root
    //|-- amount: long (nullable = true)
    //|-- pname: string (nullable = true)
    //|-- price: double (nullable = true)

    //json格式的数据类型是按照字典顺序写的，注意顺序
    jsonDF.toDF("amount2","pname2","price2").show()
    jsonDF.withColumnRenamed("pname","names").show()

    val whereDS: Dataset[Row] = jsonDF.where("price > 100")
    whereDS.write.json("jsonout")

    spark.stop()
  }
}
