package com.qwx.sql

import com.qwx.spark.MySparkUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 获取dataFrame的方式：
  * 通过RDD[类] + case class 利用反射获取schema的信息
  */
object DataFrameDemo1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    //创建SparkSQL实例
    val sqlContext = new SQLContext(sc)
    //导入隐式转换，不导入无法toDF()
    import sqlContext.implicits._
    val rdd1: RDD[String] = sc.textFile("person.txt")
    val pRDD: RDD[Person] = rdd1.map(t => {
      val split: Array[String] = t.split(" ")
      //利用Person 样例类封装数据
      Person(split(0), split(1).toInt, split(2).toDouble)
    })
    //缺少隐式转换爆红
    val pdf: DataFrame = pRDD.toDF()

    pdf.printSchema()
    //println(pdf.schema)

    //基于DataFrame执行操作：SQL / DSL
    //SQL   语法风格：若要使用就需要将DataFrame注册为一张临时表，再调用SQLContext的SQL方法，执行SQL
    pdf.registerTempTable("t_person")
    val sql: DataFrame = sqlContext.sql("select * from t_person order by sal desc")
    //默认展示20行
    sql.show()

    //DSL语法风格操作
    //DSL：领域特定语言    sql中的函数使用方法进行调用
    pdf.select("name","age").show()
    pdf.where("age > 30").show()


    //pdf.select("name").show()
    //pdf.filter("age>20").show()

    sc.stop()
  }
}
case class Person(name:String,age:Int,sal:Double)