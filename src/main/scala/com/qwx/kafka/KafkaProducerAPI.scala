package com.qwx.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaProducerAPI {
  def main(args: Array[String]): Unit = {
    val producer = KafkaProducerUtil()
    for (i <- 0 to 100000){
      //未指定分区，也未指定key，就按照默认的轮询方式写入指定分区 0 1 2 3 4
      //指定了key，就按key的hashCode写入分区
      //指定分区。就直接写入
      val partition = i % 3
      val msg = new ProducerRecord[String,String]("test1",partition,"","hello"+i)
      producer.send(msg,new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if(null != recordMetadata){
            println("发送成功"+recordMetadata.partition())
          }else{
            println("发送失败")
          }
        }
      })
    }
  }
}
