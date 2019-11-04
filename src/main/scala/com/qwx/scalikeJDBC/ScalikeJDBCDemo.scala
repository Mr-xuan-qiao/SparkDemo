package com.qwx.scalikeJDBC

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object ScalikeJDBCDemo {
  def main(args: Array[String]): Unit = {
    //加载配置文件顺序：application.conf->application.json->application.properties
    //加载数据库配置信息
    //默认加载 db.default.*的配置
    DBs.setup()
    //加载指定配置bd.ch  配置信息
    //DBs.setup('ch)

    val result:List[(Int,String,Integer)] = DB.readOnly{ implicit session =>
      SQL("select * from emp2").map(rs => {
        //索引从1开始
        val no: Int = rs.int(1)
        //按照字段名称
        val ename: String = rs.string("ename")
        val mag: Integer = rs.get[Integer](4)
        //元组封装返回结果
        (no,ename,mag)
      }).list().apply()
    }
    //result.foreach(println)

    //条件查询
    val result3:List[Int] = DB.readOnly{implicit session =>
      SQL("select * from emp2 where empno = ?").bind("22").map(rs => {
        rs.int(1)
      }).list().apply()
    }
    result3.foreach(println)
  }
}
