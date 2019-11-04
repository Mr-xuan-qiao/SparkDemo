package com.qwx.streaming.updateStage

import com.qwx.redis.JedisPoolUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object StageRedisWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val dsData: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop002",9999)
    val flatData: DStream[String] = dsData.flatMap(_.split(" +"))
    val wordWithOne: DStream[(String, Int)] = flatData.map((_,1))
    val reduceData: DStream[(String, Int)] = wordWithOne.reduceByKey(_+_)

    reduceData.foreachRDD(rdd => {
      //driver端
      rdd.foreachPartition(it => {
        //获得redis连接
        val jedis = JedisPoolUtils()
        it.foreach(tp => jedis.hincrBy("streamingwc",tp._1,tp._2))
        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
