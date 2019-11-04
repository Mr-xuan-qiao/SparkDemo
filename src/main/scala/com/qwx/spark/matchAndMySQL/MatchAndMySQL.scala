package com.qwx.spark.matchAndMySQL

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.qwx.spark.MySparkUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object MatchAndMySQL {

  //定义一个方法：将ip地址转为10进制
  def ip2Long(ip:String):Long={
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //二分法搜索
  def binarySearch(longIp: Long,ipRules :Array[(Long,Long,String)]):(String,Int) = {
    //定义起始两个指针
    var low = 0
    var high = ipRules.length-1

    while (low <= high){
      //中间索引
      val middle = (low + high) / 2
      //获取中间索引值
      val (start,end,province) = ipRules(middle)
      if(longIp >= start && longIp <=end){
        //找到
        return (province,1)
      }else if(longIp <= start){//左区间
        high = middle - 1
      }else{//右区间
        low = middle + 1
      }
    }
    //没有匹配到，默认返回值
    ("unknown",1)
  }

  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)

    //数据格式：20090121000132581311000|115.20.36.118|tj.tt98.com|/tj.htm|Mozilla/4.0 (compatible; MSIE 6.0: Windows NT 5.1; SV1; TheWorld) |
    val log: RDD[String] = sc.textFile("D:\\ipaccess.log")
    //数据格式：1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    val ip: RDD[String] = sc.textFile("D:\\ip.txt")

    //数据预处理   提取ip并转为10进制
    val longIps: RDD[Long] = log.map(str => {
      val split: Array[String] = str.split("\\|")
      val strip = split(1)
      //ip地址转为10进制
      ip2Long(strip)
    })

    //数据预处理   提取起始ip和结尾ip
    val ipRuleRdd: RDD[(Long, Long, String)] = ip.map(str => {
      val split: Array[String] = str.split("\\|")
      //起始ip和终止ip  省份
      val start: Long = split(2).toLong
      val end: Long = split(3).toLong
      val province: String = split(6)
      (start, end, province)
    })

    //两个RDD不能嵌套：map中的函数，是在executor中被调用的，   filter是在driver端调用
    //将两个10进制ip匹配
//    longIps.map(longIp => {
//      ipRuleRdd
//    })

    //解决RDD不能被嵌套：将RDD转为本地集合  (因为数据量不是很大   可用collect)
    val ipRuleRdds: Array[(Long, Long, String)] = ipRuleRdd.collect()

    val ipBc: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ipRuleRdds)

    //使用传统的for循环可解决问题，但效率低下
    //ipRules 规则数据，有序，可使用二分法查找
    val result: RDD[(String, Int)] = longIps.map(binarySearch(_,ipBc.value)).reduceByKey(_+_)

    result.foreach(println)

    //用jdbc写入到MySQL
    result.foreachPartition(it=>{
      var connection: Connection = null
      var pstm1:PreparedStatement =null
      //foreachPartition 在executor执行，不能被driver端捕获
      try {
        //DriverManager未实现序列化接口，只能写在foreachPartition内，满足闭包引用执行task
        connection = DriverManager.getConnection("xxx", "", "")
        pstm1 = connection.prepareStatement("crate table if not exists ip_access(province varchar(20),cnts int)")
        pstm1.execute()

        it.foreach(tp => {

          val pstm2: PreparedStatement = connection.prepareStatement("insert into ip_access values (?,?)")
          pstm2.setString(1, tp._1)
          pstm2.setInt(2, tp._2)
          pstm2.execute()
        })
      } catch {
        case e:Exception => e.printStackTrace()
      }finally {
        if(null != pstm1) pstm1.close()
        if(null != connection) connection.close()
      }
    })

    sc.stop()
  }
}
