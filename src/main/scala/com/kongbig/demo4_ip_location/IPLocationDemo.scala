package com.kongbig.demo4_ip_location

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import scala.collection.mutable.ArrayBuffer

/**
  * Describe: IP归属地查询(详细记录)
  * Author:   kongbig
  * Data:     2018/1/12.
  */
object IPLocationDemo {

  def main(args: Array[String]) = {
    val ip = "120.55.185.61"
    val ipNum = ip2Long(ip)
    // 读取规则库
    val lines = readData("e:/develop/spark/ip.txt")
    val index = binarySearch(lines, ipNum)
    print(lines(index))
  }

  /**
    * 二进制ip转换为十进制ip
    *
    * @param ip 二进制ip
    * @return 十进制ip
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readData(path: String) = {
    // 输入流
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
    var s: String = null
    var flag = true
    // buffer字符串类型的数组
    val lines = new ArrayBuffer[String]()
    while (flag) {
      // 读一行数据
      s = br.readLine()
      if (s != null)
        lines += s
      else
        flag = false
    }
    lines
  }

  /**
    * 二分法查找
    *
    * @param lines
    * @param ip
    * @return
    */
  def binarySearch(lines: ArrayBuffer[String], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      // 120.55.0.0|120.55.255.255|2016870400|2016935935|亚洲|中国|浙江|杭州||阿里巴巴|330100|China|CN|120.153576|30.287459
      if ((ip >= lines(middle).split("\\|")(2).toLong) && (ip <= lines(middle).split("\\|")(3).toLong))
        return middle
      if (ip < lines(middle).split("\\|")(2).toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

}
