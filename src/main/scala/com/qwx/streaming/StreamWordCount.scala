package com.qwx.streaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamWordCount {
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
    //print() 就是action算子   默认打印10行
    reduceData.print()
    //将接收器，自定义程序启动
    scc.start()
    //将该进程阻塞
    scc.awaitTermination()

    //实时程序，不能关闭
    //scc.stop()
  }
}
