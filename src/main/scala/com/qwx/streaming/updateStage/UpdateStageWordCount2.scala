package com.qwx.streaming.updateStage

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object UpdateStageWordCount2 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val ckDir = "streaming-ck"

  //存放所有的业务逻辑代码
  var creatingFunc = () => {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    ssc.checkpoint(ckDir)

    //updateStateByKey要传入的参数   updateFunc:(Seq[V],Option[S]) => Options[S]
    //历史数据 + 当前批次数据 = 历史数据
    val updateFunc = (currentData:Seq[Int],oldData:Option[Int]) => {
      //当前批次数据 + 历史数据
      Some(currentData.sum + oldData.getOrElse(0))
    }

    val dsData: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop002",9999)
    val result: DStream[(String, Int)] = dsData.flatMap(_.split(" +")).map((_,1)).updateStateByKey(updateFunc)
    result.print()

    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getOrCreate(ckDir,creatingFunc)
    ssc.start()
    ssc.awaitTermination()
  }
}
