package com.qwx.streaming.Kafka

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingKafkaDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val topics = Array("test1")
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "192.168.230.133:9092",
      "group.id" -> "test",
      "enable.auto.commit" -> "true",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"->"earliest"
    )

    //                                            Key       V
    val directStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      //均衡配置，使用所有的executors。在大多数情况下使用，它将始终在所有执行程序之间分配分区
      LocationStrategies.PreferConsistent,
      //当kafka集群和spark集群在同一个节点时用这个
      //LocationStrategies.PreferBrokers
      ConsumerStrategies.Subscribe[String,String](topics, kafkaParams))

    //报错：Object未被序列化    原因：要将ConsumerRecord从executor端拿到driver端，但ConsumerRecord底层未实现序列化
    //directStream.print()

    directStream.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
