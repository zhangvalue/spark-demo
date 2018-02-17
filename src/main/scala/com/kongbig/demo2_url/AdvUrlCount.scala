package com.kongbig.demo2_url

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: 网站访问次数统计(优解)
  * Author:   kongbig
  * Data:     2018/1/11.
  */
object AdvUrlCount {

  def main(args: Array[String]): Unit = {
    // 模拟从数据库中加载规则
    val ruleArr = Array("java.kongbig.com", "php.kongbig.com", "net.kongbig.com")

    val conf = new SparkConf().setAppName("AdvUrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 
    val rdd1 = sc.textFile("E:\\develop\\spark\\url-kongbig.log")
      .map(line => {
        val arr = line.split("\t")
        // url出现一次记录一次1
        (arr(1), 1)
      })
    val rdd2 = rdd1.reduceByKey(_ + _)
    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2) // (主机名, url, 出现次数)
    })

    //    val rddJava = rdd3.filter(_._2=="java.kongbig.com")// 过滤出java的url
    //    val sortedJava = rddJava.sortBy(_._3,false).take(3)// 按次数倒序排序取前三
    //    val rddPhp= rdd3.filter(_._1 == "php.kongbig.com")

    for (ins <- ruleArr) {
      val rddLang = rdd3.filter(_._1 == ins)
      val result = rddLang.sortBy(_._3, false).take(3) // 次数最多的top3
      // 通过调用JDBC的方法向数据库中存储数据
      // 表设计：id, 语言, URL, 次数, 访问日期
      println(result.toBuffer)
    }
    sc.stop()
  }

}
