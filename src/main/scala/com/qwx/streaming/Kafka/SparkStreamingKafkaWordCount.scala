package com.qwx.streaming.Kafka

import com.qwx.redis.JedisPoolUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingKafkaWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))

    val topics = Array("wc1")
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "192.168.230.133:9092",
      "group.id" -> "test",
      //自动提交了偏移，无法保证数据被一次消费     默认情况下，自动提交偏移
      "enable.auto.commit" -> "false",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"->"earliest"
    )

    //                                              K       V
    val directStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    //val result: DStream[(String, Int)] = directStream.map(t=>(t.value(),1)).reduceByKey(_+_)

    //写入redis
    directStream.foreachRDD(rdd => {
      //获取偏移量---一定在原生的DStream上获得偏移量
      //offsetRanges 长度等于 消费kafka的几个分区
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      ranges.foreach(r=>println(r))

      val wordRDD: RDD[(String, Int)] = rdd.map(t=>(t.value(),1)).reduceByKey(_+_)
      wordRDD.foreachPartition(part => {
        val jedis = JedisPoolUtils()
        part.foreach(tp => {
          jedis.hincrBy("streamingkafkawc",tp._1,tp._2)
        })
        jedis.close()
      })

      //将获取的偏移量提交
      //一定在DStream做提交
      directStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
    })
    //directStream.foreachRDD(rdd=>{rdd.foreach(t=>println(t.partition(),t.offset()))})
    //result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
