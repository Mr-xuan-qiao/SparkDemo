package com.qwx.redis

import redis.clients.jedis.Jedis

object JedisDemo1 {
  def main(args: Array[String]): Unit = {
    //创建jedis
    val jedis = new Jedis("hadoop001",6379)
    val result: String = jedis.get("name")
    val ping: String = jedis.ping()
    println(result,ping)

    //释放jedis
    jedis.close()
  }
}
