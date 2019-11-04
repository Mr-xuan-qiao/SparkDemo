package com.qwx.sql.dualTable

import com.qwx.spark.util.UrlUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object OrdersDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    //数据格式：B 202.106.0.20 服装 布莱尼奥西服 199
    val orderData: Dataset[String] = spark.read.textFile("D:\\orders.log")
    val ordersDF: DataFrame = orderData.map(str => {
      val split: Array[String] = str.split(" ")
      val uID: String = split(0)
      val ip:Long  = UrlUtils.ip2Long(split(1))
      val sort: String = split(2)
      val detail: String = split(3)
      val amount: Double = split(4).toDouble
      (uID, ip, sort, detail, amount)
    }).toDF("userId", "ip", "types", "pName", "price")


    val ipData: Dataset[String] = spark.read.textFile("D:\\ip.txt")
    val ipRules: DataFrame = ipData.map(str => {
      val split: Array[String] = str.split("\\|")
      val start: String = split(2)
      val end: String = split(3)
      val province: String = split(6)
      val city: String = split(7)
      (start, end, province, city)
    }).toDF("start", "end", "province", "city")

    //SQL 计算每个省下城市成交额的top3
    ordersDF.createTempView("orders")
    ipRules.createTempView("ips")
    val sql: DataFrame = spark.sql("select province,city,sum(price) cnts" +
      " from orders t1 join ips t2" +
      " on t1.ip between t2.start and t2.end " +
      "group by province,city order by cnts desc")

    sql.createTempView("temp")

    //每个省份下的城市top3
    val sql2: DataFrame = spark.sql("select province,city," +
      "row_number() over(partition by province order by cnts desc) as subOrders " +
      "from temp having subOrders <= 3")

    sql2.show()
  }
}
