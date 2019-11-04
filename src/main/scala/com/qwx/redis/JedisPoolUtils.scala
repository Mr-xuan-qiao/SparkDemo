package com.qwx.redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisPoolUtils {
  val config = new JedisPoolConfig()
  //设置参数
  //最大空闲连接
  config.setMaxIdle(5)
  //最大连接数
  config.setMaxTotal(2000)
  private[this] val pool = new JedisPool(config,"hadoop001",6379)
  //获取一个redis连接
  def apply(): Jedis = pool.getResource

  def main(args: Array[String]): Unit = {
    val jedis = pool.getResource

    jedis.ping()
  }
}
