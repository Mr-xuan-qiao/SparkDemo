package com.qwx.kafka

import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.log4j.{Level, Logger}
import sun.plugin2.jvm.RemoteJVMLauncher.CallBack

import scala.util.Random

object KafkaWcProducer {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val producer = KafkaProducerUtil()

    for(i <-0 to 1100){
      Thread.sleep(1000)
      val partition = i % 3
      val word: Int = Random.nextInt(26) + 'a'
      val msg = new ProducerRecord[String,String]("wc1",partition,"",word.toChar+"")

      producer.send(msg,new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if(null != recordMetadata){
            println(recordMetadata.hasOffset+"success")
          }
        }
      })
    }
  }
}
