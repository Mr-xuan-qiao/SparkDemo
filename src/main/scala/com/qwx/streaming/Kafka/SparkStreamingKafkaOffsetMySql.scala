package com.qwx.streaming.Kafka

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.qwx.redis.JedisPoolUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable

/**
  * 用MySQL维护偏移量
  * MySQL表存储偏移量信息
  * topic partition fromoffset(不需要) untiloffset groupId
  */
object SparkStreamingKafkaOffsetMySql {
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
    val config: Config = ConfigFactory.load()
    val conn: Connection = DriverManager.getConnection(config.getString("db.url"),config.getString("db.user"),config.getString("db.password"))
    val pstm: PreparedStatement = conn.prepareStatement("create table if not exists mysqloffset (topic varchar(20),part int,offset long,groupid varchar(20))")
    pstm.execute()
    val pstm1: PreparedStatement = conn.prepareStatement("select part,offset from mysqloffset where groupid=? and topic=?")
    pstm1.setString(1,groupId)
    pstm1.setString(2,topics(0))
    val query: ResultSet = pstm1.executeQuery()
    while (query.next()){
      val pnum: Int = query.getInt("part")
      val offset: Long = query.getLong("offset")
      //将当前分区下的所有分区偏移量 写入到集合
      offsetMp(new TopicPartition(topics(0),pnum)) = offset
    }
    conn.close()

    //                                              K       V
    val directStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsetMp))

    //val result: DStream[(String, Int)] = directStream.map(t=>(t.value(),1)).reduceByKey(_+_)

    //写入redis
    directStream.foreachRDD(rdd => {
      //获取偏移量---一定在原生的DStream上获得偏移量
      //offsetRanges 长度等于 消费kafka的几个分区
      val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
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
      val conn: Connection = DriverManager.getConnection(config.getString("db.url"),config.getString("db.user"),config.getString("db.password"))
      val pstm2: PreparedStatement = conn.prepareStatement("replace into mysqloffset values (?,?,?,?)")
      ranges.foreach(t=>{
        pstm2.setString(1,topics(0))
        pstm2.setInt(2,t.partition)
        pstm2.setLong(3,t.untilOffset)
        pstm2.setString(4,groupId)
        pstm2.execute()
      })
      conn.close()

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
