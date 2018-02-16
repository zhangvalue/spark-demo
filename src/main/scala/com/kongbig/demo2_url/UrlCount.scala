package com.kongbig.demo2_url

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: 网站访问次数统计(分组top3)
  * Author:   kongbig
  * Data:     2018/1/11.
  */
object UrlCount {

  // 20160321101954	http://java.kongbig.com/java/course/javaeeadvanced.shtml
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // rdd1将数据切分、元组中方的是(URL, 1)
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
      (host, url, t._2) // (主机名, url ,出现次数)
    })

    // 按host名分组
    val rdd4 = rdd3.groupBy(_._1)
    val rdd5 = rdd4.mapValues(it => {
      // 倒序排序取top3
      it.toList.sortBy(_._3).reverse.take(3)
    })

    println(rdd5.collect().toBuffer)
    sc.stop()
  }
}
