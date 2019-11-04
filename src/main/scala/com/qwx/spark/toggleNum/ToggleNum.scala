package com.qwx.spark.toggleNum

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

object ToggleNum {
  def main(args: Array[String]): Unit = {
    //数据格式： 1010,华语剧场|剧情|当代|类型,1,0
    val sc = MySparkUtil(this.getClass.getSimpleName)

    //读取数据
    val file: RDD[String] = sc.textFile("D:\\zx.txt")

    //数据切分
    //1010,华语剧场|剧情|当代|类型,1,0
    //flatMap用于嵌套map
    val processData: RDD[((String, String), (Int, Int))] = file.flatMap(str => {
      val split = str.split(",")
      val id = split(0)
      val words = split(1)
      val imp = split(2).toInt
      val click = split(3).toInt
      //对关键词切分
      val split1 = words.split("\\|")

      split1.map(t => ((id, t), (imp, click)))
    })

    //processData.collect().foreach(println)

    //简单方法获得
    val result1: RDD[((String, String), (Int, Int))] = processData.reduceByKey {
      case (v1, v2) => (v1._1 + v2._2, v2._1 + v1._2)
    }

    //分组聚合方法获得
    //分组
    val groupedRDD: RDD[((String, String), Iterable[(Int, Int)])] = processData.groupByKey()
    //聚合
    val result2: RDD[((String, String), (Int, Int))] = groupedRDD.mapValues(it => {
      val totalTemp = it.map(_._1).sum
      val totalClick = it.map(_._2).sum

      (totalTemp, totalClick)
    })

    //整合处理结果
    val value: RDD[(String, String, Int, Int)] = result2.map{case ((id, t), (imp, click)) => (id, t,imp, click)}
    val sortValue: RDD[(String, String, Int, Int)] = value.sortBy(+_._4)

    sortValue.foreach(println)

  }
}
