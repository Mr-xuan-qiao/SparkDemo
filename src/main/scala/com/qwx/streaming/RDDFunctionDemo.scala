package com.qwx.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object RDDFunctionDemo {
  //屏蔽过多的日志
  Logger.getLogger(("org")).setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    //Streaming程序必须有一个新的入口   批次时间必须设置在StreamingContext中
    val scc = new StreamingContext(sc,Seconds(2))

    //读取socket数据
    val dsData: ReceiverInputDStream[String] = scc.socketTextStream("hadoop002",9999)
    val flatData: DStream[String] = dsData.flatMap(_.split(" +"))
    val wordWithOne: DStream[(String, Int)] = flatData.map((_,1))
    val reduceData: DStream[(String, Int)] = wordWithOne.reduceByKey(_+_)

    //每个RDD的元素数量
    val cnt: DStream[Long] = reduceData.count()
    //cnt.print()

    //返回一个(K,Long)对的新DStream
    val cntv: DStream[((String, Int), Long)] = reduceData.countByValue()
    cntv.print()
    //((a,2),1)
    //((b,1),1)
    //((c,1),1)

    //RDD - RDD
    val transform: DStream[Int] = reduceData.transform(rdd => rdd.map(_._2))

    //DStream是RDD + 时间戳
    //使用
    dsData.foreachRDD((rdd,time) => {
      val result: RDD[(String, Int)] = rdd.flatMap(_.split(" +")).map((_,1)).reduceByKey(_ + _)
      rdd.foreach(println)
      result.foreach(println)
    })

    //输出到文件
    reduceData.saveAsTextFiles("t1","log")

    //将接收器，自定义程序启动
    scc.start()
    //将该进程阻塞
    scc.awaitTermination()
  }
}
