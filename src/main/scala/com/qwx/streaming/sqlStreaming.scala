package com.qwx.streaming

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object sqlStreaming {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    
    val dsData: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop002",9999)
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import session.implicits._

    //加载默认配置文件的顺序 application.conf--->application.json--->application.properties
    val config: Config = ConfigFactory.load()

    dsData.foreachRDD(rdd => {
      val ds: Dataset[String] = session.createDataset(rdd)
      val words: Dataset[String] = ds.flatMap(_.split(" +"))
      //(string,int)   _1 _2
      //保存模式  ，否则会出现表已存在
      ds.createOrReplaceTempView("wc")
      val result: DataFrame = session.sql("select value,count(*) cnts from wc group by value order by cnts desc")

      val conn = new Properties()
      conn.setProperty("user",config.getString("db.user"))
      conn.setProperty("password",config.getString("db.password"))
      //在集群运行必须加Driver
      conn.setProperty("driver","com.mysql.jdbc.Driver")

      result.write.jdbc(config.getString("db.url"),"streamingdb",conn)
      //result.show()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
