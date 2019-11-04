package com.qwx.sql.datasource

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql._

object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    val config: Config = ConfigFactory.load()

    val conn = new Properties()
    conn.setProperty("user",config.getString("db.user"))
    conn.setProperty("password",config.getString("db.password"))
    conn.setProperty("driver",config.getString("db.driver"))

    //读取jdbc的API
    val jdbc: DataFrame = spark.read.jdbc(config.getString("db.url"),"bank_a",conn)
    spark.read.format("jdbc").jdbc(config.getString("db.url"),"bank_a",conn)
    jdbc.printSchema()

    val selectDF: DataFrame = jdbc.where("money < 200").select("user")
    selectDF.printSchema()

    selectDF.write.mode(SaveMode.Append).jdbc(config.getString("db.url"),"emp2",conn)

    spark.stop()
  }
}
