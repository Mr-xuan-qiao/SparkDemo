package com.qwx.sql.datasource

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CSVDataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    val csv: DataFrame = spark.read.csv("log.csv")
    csv.printSchema()
    //root
    //|-- _c0: string (nullable = true)
    //|-- _c1: string (nullable = true)
    //|-- _c2: string (nullable = true)
    csv.show()

    val csvDS: DataFrame = csv.map(str => {
      val name: String = str.getString(0)
      val age: Int = str.getString(1).toInt
      val fv: Double = str.getString(2).toDouble
      (name, age, fv)
    }).toDF("name", "age", "fv")

    val filter: Dataset[Row] = csvDS.where("age>30")
    filter.show()
    filter.write.csv("csvout")

    spark.stop()
  }
}
