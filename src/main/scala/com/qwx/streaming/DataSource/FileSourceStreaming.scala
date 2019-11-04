package com.qwx.streaming.DataSource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object FileSourceStreaming {
  Logger.getLogger(("org")).setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    //Streaming程序必须有一个新的入口   批次时间必须设置在StreamingContext中
    val ssc = new StreamingContext(sc,Seconds(2))

    val fileStream: DStream[String] = ssc.textFileStream("F:\\mrdata\\testLog")
    fileStream.flatMap(_.split(" +")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
