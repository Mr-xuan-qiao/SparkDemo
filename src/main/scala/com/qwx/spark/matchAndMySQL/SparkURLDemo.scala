package com.qwx.spark.matchAndMySQL

import java.net.URL

import com.qwx.spark.MySparkUtil
import com.qwx.spark.util.UrlUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * 文件url.db 文件数据格式为：MD5(url不含http://)+type
  * 文件40690.txt，文件中数据格式为 url(MD5加密的)+type
  * 需求：40096文件中的url地址经过加密后，如果跟url.db文件中的相同，则将url.db中的type更新为40096中的type,不同不做修改
  */
object SparkURLDemo {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)
    //数据格式：http://007sn.com	# 20
    val orgData: RDD[String] = sc.textFile("D:\\40096.txt")
    //数据格式：
    val url: RDD[String] = sc.textFile("D:\\url.db1000")

    //数据预处理
    val orgProcess: RDD[(String, String)] = orgData.map(t => {
      val split: Array[String] = t.split("\t#\t")
      //获取url 去除http://前缀
      val host: String = new URL(split(0)).getHost
      //加密
      val md5Url: String = UrlUtils.md5Encoding(host)

      val types = split(1)
      //因为类型长度问题，类型<10，需补全两位类型
      val newTypes: String = types.length match {
        case 1 => "0" + types
        case 2 => types
      }
      (md5Url, newTypes)
    })

    val urlProcess: RDD[(String, String)] = url.map(str => {
      val md5Url: String = str.substring(0, 14)
      val types: String = str.substring(14)
      (md5Url, types)
    })
    //              key     左值          右值
    val join: RDD[(String, (String, Option[String]))] = urlProcess.leftOuterJoin(orgProcess)

    //方案1 ：map实现
    join.map{
      case (md5Url, (urlTypes,orgTypes)) => {
        //最简单的判断，如果orgTypes有值，为orgTypes     orgTypes无值，返回urlTypes
        orgTypes.getOrElse("old"+urlTypes)
        //方式2：
        /*orgTypes match {
          case Some(v) => v
          case None => urlTypes
        }*/
      }
    }//.foreach(println)

    //方案2  广播变量实现
    val bc: Broadcast[collection.Map[String, String]] = sc.broadcast(orgProcess.collectAsMap())
    urlProcess.map(str => {
      //原始数据中的                key    value
      val value: collection.Map[String, String] = bc.value

      (str._1,value.getOrElse(str._1,str._2))
    })
        .foreach(println)


    sc.stop()
  }
}
