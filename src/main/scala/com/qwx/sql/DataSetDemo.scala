package com.qwx.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataSetDemo {
  def main(args: Array[String]): Unit = {
    //spark 2.x统一了访问api
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    //一定导入隐式转换
    import spark.implicits._
    val file: Dataset[String] = spark.read.textFile("person.txt")
    //dataSet自带schema
    file.printSchema()
    //root
    //|-- value: string (nullable = true)

    //数据处理：Dataset 元组的schema信息是根据 元组类型来确定的
    val splitDS: Dataset[(String, Int, Double)] = file.map(str => {
      val strs: Array[String] = str.split(" ")
      (strs(0), strs(1).toInt, strs(2).toDouble)
    })
    splitDS.printSchema()
    //看到，元组类型的取值只能用 _1._2._3    因此还要自定义schema
    //root
    //|-- _1: string (nullable = true)
    //|-- _2: integer (nullable = false)
    //|-- _3: double (nullable = false)

    //自定义schema
    val pdf: DataFrame = splitDS.toDF("name","age","sal")
    val rdd: RDD[Row] = pdf.rdd
    val rdd3: RDD[(String, Int, Double)] = rdd.map(row => {
      //row.get(0)
      val name: String = row.getAs[String](0)
      val age: Int = row.getAs[Int](1)
      val sal: Double = row.getDouble(2)
      (name, age, sal)
    })
    rdd3.foreach(println)

    //SQL风格
    pdf.createTempView("v_person")
    val sql: DataFrame = spark.sql("select * from v_person where age > 30 limit 1")
    sql.show()

    //DSL风格：
    //select 选择
    pdf.select("name","age").show()
    //过滤  filter  where   where 调用了filter，是同一个api
    pdf.filter("age > 30").show()
    //order by     sort 是同一个api    默认升序
    pdf.orderBy($"age"desc).show()
    //分组groupBy
    val cnt: DataFrame = pdf.groupBy("age").count()
    cnt.show()
    val maxdf: DataFrame = pdf.groupBy("age").max("sal")
    maxdf.show()
    //agg聚合需要导入函数包
    import org.apache.spark.sql.functions._
    pdf.groupBy("age").agg(count("*") as "cnts").show()
    pdf.groupBy("age").agg(sum("sal") as "sumSal").show()
    maxdf.withColumnRenamed("max(sal)","maxSal").sort($"age"desc).limit(4).show()

    spark.close()
  }
}
