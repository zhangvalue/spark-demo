package com.kongbig.demo2_url

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Describe: 网站访问次数统计(自定义分区)
  * Author:   kongbig
  * Data:     2018/2/18.
  */
object UrlCountPartition {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // rdd1将数据切分、元组中放的是(url, 1)
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
      // (主机名, (url, 次数))
      (host, (url, t._2))
    }).cache() // cache会将数据缓存到内存当中，cache是一个Transformation（可以提高性能）

    // 按host分组，倒排，取top3
    //    val rdd4 = rdd3.groupBy(_._1)
    //    val rdd5 = rdd4.mapValues(it => {
    //      it.toList.sortBy(_._2._2).reverse.take(3)
    //    })
    //    println(rdd5.collect().toBuffer)

    val ints = rdd3.map(_._1).distinct().collect()
    val hostPartitioner = new HostPartitioner(ints)

    // 用HashPartitioner要知道需要多少个分区
    //    val rdd4 = rdd3.partitionBy(new HashPartitioner(ints.length))
    //    println(rdd4.collect().toBuffer)

    // 采用自定义的partitioner
    val rdd4 = rdd3.partitionBy(hostPartitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
    println(rdd4.collect().toBuffer)

    //    rdd4.saveAsTextFile("e://out3")
    sc.stop()
  }

}

/**
  * 决定了数据到哪个分区里面
  *
  * @param ins
  */
class HostPartitioner(ins: Array[String]) extends Partitioner {
  val partMap = new mutable.HashMap[String, Int]()
  var count = 0
  for (i <- ins) {
    partMap += (i -> count)
    count += 1
  }

  // 分区数量
  override def numPartitions: Int = ins.length

  // 根据key获取分区num
  override def getPartition(key: Any): Int = {
    // 没有匹配的key就返回0
    partMap.getOrElse(key.toString, 0)
  }
}