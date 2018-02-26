package com.kongbig.demo8_streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: SparkStreaming的wordCount例子
  * Author:   kongbig
  * Data:     2018/1/14 17:54.
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    // 设置日志级别
    LoggerLevels.setStreamingLogLevels()

    // local[2]：本地运行模式和开两个线程
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // StreamingContext(5秒钟一个批次)
    val ssc = new StreamingContext(sc, Seconds(5))

    /**
      * 接收数据
      * 生产者：192.168.33.61:8888，通过TCP将数据拉过来
      */
    val ds = ssc.socketTextStream("192.168.33.61", 8888)
    // DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    // 打印结果
    result.print()

    // 启动StreamingContext
    ssc.start()
    // 等待着结束
    ssc.awaitTermination()
  }

  /**
    * 在Linux端命令行中输入单词：
    * 命令：nc -lk 8888
    * 输入要统计的单词：hello tom hello jerry hello tom...
    */
}
