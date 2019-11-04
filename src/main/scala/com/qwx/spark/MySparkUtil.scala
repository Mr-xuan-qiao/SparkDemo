package com.qwx.spark

import org.apache.spark.{SparkConf, SparkContext}

object MySparkUtil {

  //获得本地的SparkContext
  def apply(appName:String): SparkContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(appName)
    new SparkContext(conf)
  }
}
