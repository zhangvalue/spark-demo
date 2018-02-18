package com.kongbig.demo3_user_location

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: 计算用户在小区停留时间最长的两个小区
  * 通过基站信息计算家庭地址和工作地址
  * Author:   kongbig
  * Data:     2018/1/11.
  */
object UserLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 数据格式：(手机号_基站id)
    val mobileAndVisitLocationAndTime = sc.textFile("E:\\develop\\spark\\bs_log")
      .map(line => {
        val fields = line.split(",")
        // 1代表与基站建立连接; 0代表与基站释放连接;
        val eventType = fields(3)
        val time = fields(1)
        // 与基站建立连接设置时间为负，反之为正。
        val timeLong = if (eventType == "1") -time.toLong else time.toLong
        // 元组：(手机号_基站id, 时间)
        (fields(0) + "_" + fields(2), timeLong)
      })
    // 1.按手机号_基站id分组; 2.对分组后的每一个key值所对应的全部values值相加
    val rdd1 = mobileAndVisitLocationAndTime.groupBy(_._1).mapValues(_.foldLeft(0L)(_ + _._2))
    // println(rdd1.collect().toBuffer)
    val rdd2 = rdd1.map(t => {
      val mobile_bs = t._1
      // 手机号
      val mobile = mobile_bs.split("_")(0)
      // 基站id
      val lac = mobile_bs.split("_")(1)
      val time = t._2
      (mobile, lac, time) // 放到元组中
    })
    // 按手机号进行分组
    val rdd3 = rdd2.groupBy(_._1)
    val rdd4 = rdd3.mapValues(it => {
      // 按第三个元素排序(倒序)，取top2
      it.toList.sortBy(_._3).reverse.take(2)
    })
    // 可写入数据库
    println(rdd4.collect().toBuffer)
    sc.stop()
  }

}
