package com.kongbig.demo1_wc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: WordCount程序、远程调试
  * Author:   kongbig
  * Data:     2018/1/9.
  */
object WordCount {

  /**
    * SparkContext对象是提交Spark程序的入口
    * testFile(hdfs://mini1:9000/words.txt)是hdfs中读取数据
    * flatMap(_.split(" "))先以" "进行分割再压平
    * map((_,1))将单词和1构成元组
    * reduceByKey(_+_)按照key进行reduce，并将value累加
    * saveAsTextFile("mini1:9000/out")将结果写入到hdfs中
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // SparkContext是通向Spark集群的入口
    val conf = new SparkConf().setAppName("WC")
      .setJars(Array("E:\\IdeaProjects\\java_code\\spark-demo\\target\\spark-demo-1.0.jar"))
      .setMaster("spark://192.168.33.61:7077")
    val sc = new SparkContext(conf)

    // textFile会产生两个RDD 1.HadoopRDD -> MapPartitionsRDD
    sc.textFile(args(0))
      .flatMap(_.split(" ")) // 产生一个RDD：MapPartitionsRDD
      .map((_, 1)) // 产生一个RDD：MapPartitionsRDD
      .reduceByKey(_ + _) // 产生一个RDD：ShuffleRDD
      .saveAsTextFile(args(1)) // 产生一个RDD：MapPartitionsRDD
    sc.stop()
  }
  
}
