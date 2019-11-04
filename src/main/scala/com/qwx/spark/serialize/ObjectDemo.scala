package com.qwx.spark.serialize

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import com.qwx.spark.MySparkUtil
import org.apache.spark.util.LongAccumulator

object ObjectDemo {
  def main(args: Array[String]): Unit = {
    val sc = MySparkUtil(this.getClass.getSimpleName)
    val stream = new ObjectOutputStream(new FileOutputStream("f://person.txt"))
    val p1 = new Person("ss",11)
    stream.writeObject(p1)
    stream.writeInt(10)
    stream.flush()

    val out = new ObjectInputStream(new FileInputStream("f://person.txt"))

    //读取object的时间
    val acc: LongAccumulator = sc.longAccumulator("readObjectTime")
    val t1: Long = System.currentTimeMillis()
    val obj = out.readObject()
    val t2: Long = System.currentTimeMillis()
    acc.add(t2 - t1)
    println(acc.value)

    val p2: Person = obj.asInstanceOf[Person]
    //返回结果为false，因为反序列化后不在同一个内存地址
    println(p1 == p2)
    println(out.readInt())
  }
}

class Person(val name:String,val age:Int) extends Serializable {

  override def toString = s"Person($name, $age)"
}
