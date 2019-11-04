package com.qwx.streaming.updateStage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object UpdateStageWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc:StreamingContext = new StreamingContext(sc, Seconds(2))
    //设置checkpoint，不设置报错
    ssc.checkpoint("streaming-ck")

    //读取socket数据
    val dsData: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop002",9999)

    //updateStateByKey要传入的参数   updateFunc:(Seq[V],Option[S]) => Options[S]
    //历史数据 + 当前批次数据 = 历史数据
    val updateFunc = (currentData:Seq[Int],oldData:Option[Int]) => {
      //当前批次数据 + 历史数据
      Some(currentData.sum + oldData.getOrElse(0))
    }

    val result: DStream[(String, Int)] = dsData.flatMap(_.split(" +")).map((_,1)).updateStateByKey(updateFunc)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
