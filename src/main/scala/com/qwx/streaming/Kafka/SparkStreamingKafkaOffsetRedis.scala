package com.qwx.streaming.Kafka

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util

import com.qwx.redis.JedisPoolUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 用Redis维护偏移量
  * 数据结构用hash
  * topic partition fromoffset(不需要) untiloffset groupId
  * topic-groupId  partition offset
  */
object SparkStreamingKafkaOffsetRedis {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val topics = Array("wc1")
    val groupId = "test"
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "192.168.230.133:9092",
      "group.id" -> groupId,
      //自动提交了偏移，无法保证数据被一次消费     默认情况下，自动提交偏移
      "enable.auto.commit" -> "false",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest"
    )

    //存储了Offset的集合
    val offsetMp = mutable.HashMap[TopicPartition,Long]()
    val jedis = JedisPoolUtils()
    val all: util.Map[String, String] = jedis.hgetAll(topics(0)+"-"+groupId)

    //                                              K       V
    val directStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsetMp))

    //写入redis或mysql
    directStream.foreachRDD(rdd => {
      //获取偏移量---一定在原生的DStream上获得偏移量
      //offsetRanges 长度等于 消费kafka的几个分区
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges.filter(os => os.fromOffset != os.untilOffset)
      ranges.foreach(r => println(r))

      val wordRDD: RDD[(String, Int)] = rdd.map(t => (t.value(), 1)).reduceByKey(_ + _)
      wordRDD.foreachPartition(part => {
        val jedis = JedisPoolUtils()
        part.foreach(tp => {
          jedis.hincrBy("streamingkafkawc", tp._1, tp._2)
        })
        jedis.close()
      })
      //提交偏移量
      val jedis = JedisPoolUtils()
      ranges.foreach(os=>{
        jedis.hset(os.topic+"-"+groupId,os.partition.toString,os.untilOffset.toString)
      })
      jedis.close()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
