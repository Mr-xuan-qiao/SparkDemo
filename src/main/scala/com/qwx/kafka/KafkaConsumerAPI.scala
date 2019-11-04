package com.qwx.kafka

import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

object KafkaConsumerAPI {
  def main(args: Array[String]): Unit = {
    val consumer = KafkaConsumerUtil()

    val topic: util.List[String] = util.Arrays.asList("test1")
    //订阅一个主题
    consumer.subscribe(topic)

    while (true) {
      val poll: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(2))
      val msgItr: util.Iterator[ConsumerRecord[String, String]] = poll.iterator()
      while(msgItr.hasNext){
        val record: ConsumerRecord[String, String] = msgItr.next()
        println(record)
      }
    }
  }
}
