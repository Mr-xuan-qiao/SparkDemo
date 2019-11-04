package com.qwx.streaming.updateStage

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.log4j.Level
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StageByMySQLWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val scc = new StreamingContext(sc,Seconds(2))

    val rsData: ReceiverInputDStream[String] = scc.socketTextStream("hadoop002",9999)
    val flatData: DStream[String] = rsData.flatMap(_.split(" +"))
    val wordWithOne: DStream[(String, Int)] = flatData.map((_,1))
    val reduceData: DStream[(String, Int)] = wordWithOne.reduceByKey(_+_)

    reduceData.foreachRDD(rdd => {
      //这是driver端
      val config: Config = ConfigFactory.load()
      //RDD 非空触发写操作
      if(!rdd.isEmpty()){
        rdd.foreachPartition(it => {
          val conn: Connection = DriverManager.getConnection(config.getString("db.url"),config.getString("db.user"),config.getString("db.password"))
          val tableName:String = config.getString("db.table")
          //创建表
          val pstm1: PreparedStatement = conn.prepareStatement(s"create table if not exists ${tableName} (word varchar(20),cnts int)")
          pstm1.execute()
          pstm1.close()

          it.foreach(tp => {
            //当前的单词和出现的次数
            val currentWord: String = tp._1
            val curData: Int = tp._2

            //根据单词名称查询次数
            val pstm2: PreparedStatement = conn.prepareStatement(s"select cnts from ${tableName} where word = ?")
            pstm2.setString(1,currentWord)
            val query: ResultSet = pstm2.executeQuery()
            //有值就取值，和当前批次的值进行累加
            if(query.next()){
              val oldData: Int = query.getInt("cnts")
              val newData = oldData + curData
              //修改数据
              val pstm3: PreparedStatement = conn.prepareStatement(s"update ${tableName} set cnts = ? where word = ?")
              pstm3.setInt(1,newData)
              pstm3.setString(2,currentWord)
              pstm3.execute()
            }else{//新的key,直接插入
              val pstm4: PreparedStatement = conn.prepareStatement(s"insert into ${tableName} values(?,?)")
              pstm4.setString(1,currentWord)
              pstm4.setInt(2,curData)
              pstm4.execute()
            }
          })
          //需进行try/catch
          conn.close()
          //需进行关流
        })
      }
    })

    scc.start()
    scc.awaitTermination()
  }
}
