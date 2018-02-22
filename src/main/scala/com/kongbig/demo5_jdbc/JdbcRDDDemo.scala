package com.kongbig.demo5_jdbc

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: JDBC的demo
  * Author:   kongbig
  * Data:     2018/1/12.
  */
object JdbcRDDDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/big_data?useUnicode=true&characterEncoding=utf8", "root", "123")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM ta WHERE id >= ? AND id <= ?",
      // 1,4：条件的开始和结束
      // 2：number partition
      1, 4, 2,
      r => {
        // 处理返回值
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )
    val jRDD = jdbcRDD.collect()
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }

}
