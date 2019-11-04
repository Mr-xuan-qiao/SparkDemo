package com.qwx.sql.udf

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import jdk.nashorn.internal.objects.annotations.Property
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    val dataDF: DataFrame = spark.createDataset(List((10, 20, 30), (9, 16, 42), (12, 23, 35))).toDF("age1", "age2", "age3")
    dataDF.show()

    //注册udf函数
    spark.udf.register("operation",(x:Int)=>{
      x.toInt+100
    })
    //创建临时表
    dataDF.createTempView("xx")
    //使用函数查询
    val sql: DataFrame = spark.sql("select age2, operation(age1) as agg from xx")
    sql.show()

    /*//加载默认配置文件的顺序 application.conf--->application.json--->application.properties
    val config: Config = ConfigFactory.load()

    val url = config.getString("db.url")
    val conn = new Properties()
    conn.setProperty("user","root")
    conn.setProperty("password","123456")
    //在集群运行必须加Driver
    conn.setProperty("driver","com.mysql.jdbc.Driver")
    sql.write.mode(SaveMode.Append).jdbc(url,"tableName",conn)*/

    spark.close()
  }
}
