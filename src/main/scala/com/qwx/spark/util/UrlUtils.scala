package com.qwx.spark.util
import java.security.MessageDigest

object UrlUtils {
  def main(args: Array[String]): Unit = {
    //println(urltoMD5("cast.org.cn").substring(0,14))
    //println(urltoMD5("cast.org.cn"))
    println(md5Encoding("007sn.com"))
  }
  //定义一个方法：将ip地址转为10进制
  def ip2Long(ip:String):Long={
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }


  //得到一个信息摘要
  def md5Encoding(password:String):String={
    val digest = MessageDigest.getInstance("md5")
    val result: Array[Byte] = digest.digest(password.getBytes)
    val buffer = new StringBuffer()
    //把一个byte做一个与运算 0xff;
    for(b <- result){
      val number = b & 0xff
      val str = Integer.toHexString(number)
      if(str.length == 1) buffer.append("0")
      buffer.append(str)
    }
    //标准的MD5加密结果
    buffer.toString
  }

  //将uRL转化为MD5加密
  def urltoMD5(url :String): String={
    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    md5.update(url.getBytes())
    //加密后的字符串
    val digest: Array[Byte] = md5.digest()
    //字节数组转为12进制
    toHexString(digest)
  }

  //转16进制
  def toHexString(b: Array[Byte])={
    val hexChar = Array('1','2','3','4','5','6','7','8','9','a','b','c','d','e','f')
    val sb = new StringBuilder(b.length * 2)
    var i = 0
    while (i <= b.length){
      {
        sb.append(hexChar((b(i) & 0xf0) >>> 4))
        sb.append(hexChar(b(i) & 0x0f))
      }
      {
        i += 1
        i-1
      }
    }
    sb.toString
  }
}
