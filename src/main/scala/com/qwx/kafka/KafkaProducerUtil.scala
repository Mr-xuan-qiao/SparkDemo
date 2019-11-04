package com.qwx.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

object KafkaProducerUtil {
  def apply(): KafkaProducer[String, String] = {
    val props = new Properties()
    // Kafka 服务端的主机名和端口号
    props.setProperty("bootstrap.servers", "192.168.230.132:9092,192.168.230.131:9092,192.168.230.133:9092")
    // 等待所有副本节点的应答
    props.setProperty("acks", "all")
    // 消息发送最大尝试次数
    props.setProperty("retries", "0")
    // 一批消息处理大小
    props.setProperty("batch.size", "16384")
    // 增加服务端请求延时
    props.setProperty("linger.ms", "1")
    // 发送缓存区内存大小
    props.setProperty("buffer.memory", "33554432")
    // key 序列化
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value 序列化
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](props)

    kafkaProducer
  }
}
