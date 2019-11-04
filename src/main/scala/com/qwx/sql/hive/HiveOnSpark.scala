package com.qwx.sql.hive

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      //开启hive支持
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    //查询hive表
    val sql: DataFrame = spark.sql("select * from test3")
    sql.show()

    spark.sql("select username,month,sum(salary) as salary from t_access_times group by username,month")


    //写入自定义数据
    val access: Dataset[String] = spark.createDataset(List("b,2015-01,10","c,2015-02,20"))




    spark.close()
  }
}
