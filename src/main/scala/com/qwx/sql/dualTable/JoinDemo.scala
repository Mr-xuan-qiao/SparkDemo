package com.qwx.sql.dualTable

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    import spark.implicits._
    //用户信息表
    val ds1: Dataset[String] = spark.createDataset((List("ty 28 hunan","ruhua 40 xg","mr 30 shanxi","yixx 32 beijing","cangls 18 japan")))
    //数据预处理
    val processDS1: DataFrame = ds1.map(str => {
      val split: Array[String] = str.split(" ")
      (split(0), split(1).toInt, split(2))
    }).toDF("name", "age","proCode")

    val province: Dataset[String] = spark.createDataset((List("hunan,湖南省","xg,香港","shanxi,陕西省","beijing,北京","japan,日本省")))
    val processDS2: DataFrame = province.map(str => {
      val split: Array[String] = str.split(",")
      (split(0), split(1))
    }).toDF("proCode", "pName")

    //SQL:
    processDS1.createTempView("user")
    processDS2.createTempView("address")
    val sql1: DataFrame = spark.sql("select user.name as name,user.age,address.pName from user,address where user.proCode = address.proCode")
    val sql2: DataFrame = spark.sql("select u.age as age,u.name as name,a.pName as pName from user u join address a on a.proCode = u.proCode")
    sql1.show()
    sql2.show()

    //DSL
    val sql3: DataFrame = processDS1.join(processDS2,"proCode")
    val sql4: Dataset[Row] = processDS1.join(processDS2).where(processDS1("proCode") === processDS2("proCode")).orderBy(processDS1("age"))
    sql3.show()
    sql4.show()

    spark.stop()
  }
}
