package com.qwx.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

object KafkaConsumerUtil {
  def apply():KafkaConsumer[String,String]  = {
    val props = new Properties
    // 定义 kakfa 服务的地址，不需要将所有 broker 指定上
    props.put("bootstrap.servers", "192.168.230.133:9092")
    // 制 定 consumer group
    props.put("group.id", "test")
    // 是否自动确认 offset
    props.put("enable.auto.commit", "true")
    // 自动确认 offset 的时间间隔
    props.put("auto.commit.interval.ms", "1000")
    // key 的序列化类
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // value 的序列化类
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String,String](props)
    consumer
  }
}
