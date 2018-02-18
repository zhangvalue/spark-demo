package com.kongbig.demo3_user_location

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: 计算用户在小区停留时间最长的两个小区(优解)
  * Author:   kongbig
  * Data:     2018/1/11.
  */
object AdvUserLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdvUserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 一行log数据如下：18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
    // 数据格式：(手机号_基站id)
    val rdd0 = sc.textFile("E:\\develop\\spark\\bs_log")
      .map(line => {
        val fields = line.split(",")
        // 1代表与基站建立连接; 0代表与基站释放连接;
        val eventType = fields(3)
        val time = fields(1)
        // 与基站建立连接设置时间为负，反之为正
        val timeLong = if (eventType == "1") -time.toLong else time.toLong
        // 元组：((手机号, 基站id), 时间)
        ((fields(0), fields(2)), timeLong)
      })

    val rdd1 = rdd0.reduceByKey(_ + _).map(t => {
      val mobile = t._1._1
      val lac = t._1._2
      val time = t._2
      // (基站id, (手机号, 时间))
      (lac, (mobile, time))
    })

    val rdd2 = sc.textFile("E:\\develop\\spark\\loc_info.txt")
      .map(line => {
        val f = line.split(",")
        // (基站id, (纬度, 经度))
        (f(0), (f(1), f(2)))
      })

    // rdd1.join(rdd2)：(16030401EAFB68F1E3CDF819735E1C66,((18611132889,97500),(116.296302,40.032296)))
    val rdd3 = rdd1.join(rdd2).map(t => {
      // 基站id
      val lac = t._1
      val mobile = t._2._1._1
      val time = t._2._1._2
      // 纬度
      val x = t._2._2._1
      // 经度
      val y = t._2._2._2
      (mobile, lac, time, x, y)
    })

    // 按手机号分组
    val rdd4 = rdd3.groupBy(_._1)
    val rdd5 = rdd4.mapValues(it => {
      // 取top2
      it.toList.sortBy(_._3).reverse.take(2)
    })

    println(rdd5.collect().toBuffer)
    // rdd5.saveAsTextFile("e://out")
    sc.stop()
  }

}
