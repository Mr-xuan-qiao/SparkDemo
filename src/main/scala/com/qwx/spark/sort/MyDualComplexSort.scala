package com.qwx.spark.sort

import com.qwx.spark.MySparkUtil
import org.apache.spark.rdd.RDD

object MyDualComplexSort {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)
    val strings: List[String] = List[String]("shouji 5999 1000","shoulei 199 3","shoukao 200 10","lazhu 3 1000","feizao 4.5 1000")
    val data: RDD[String] = sc.makeRDD(strings)

    //数据预处理利用类来封装数据
    val formatData: RDD[Products] = data.map(t => {
      val proName: String = t.split(" ")(0)
      val price: Double = t.split(" ")(1).toDouble
      val saleNum: Int = t.split(" ")(2).toInt
      //new Products(proName, price, saleNum)
      new Products(proName, price, saleNum)
    })

    //直接按照类来排序
    val sortedData: RDD[Products] = formatData.sortBy(t=>t)
    sortedData.coalesce(1).foreach(println)

    sc.stop()
  }
}

//case class （样例类）默认实现序列化，故无需实现系列化，还可以不用new
case class Products (val name:String,val price:Double,val saleNum:Int) extends Ordered[Products] /*with Serializable*/ {
  //自定义的排序规则
  override def compare(that: Products): Int = {
    this.saleNum - that.saleNum
  }
  override def toString = s"Products($name, $price, $saleNum)"
}