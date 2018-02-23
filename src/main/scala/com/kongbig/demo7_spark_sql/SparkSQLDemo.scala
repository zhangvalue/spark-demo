package com.kongbig.demo7_spark_sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: SparkSQL Demo
  * Author:   kongbig
  * Data:     2018/1/213.
  */
object SparkSQLDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQLDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name", "bigdata")

    val personRDD = sc.textFile("hdfs:192.168.33.61:9000/person.txt").map(line => {
      val fields = line.split(",")
      Person(fields(0).toLong, fields(1), fields(2).toInt)
    })

    // 导入隐式转换，如果不导入无法将RDD转换成DataFrame
    // 将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF

    // 注册成临时表
    personDF.registerTempTable("person")
    sqlContext.sql("select * from person where age >= 20 order by age desc limit 2").show()
    sc.stop()
  }

}

case class Person(id: Long, name: String, age: Int)