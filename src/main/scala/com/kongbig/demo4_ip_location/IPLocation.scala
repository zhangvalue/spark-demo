package com.kongbig.demo4_ip_location

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Describe: IP归属地查找(将结果写入MySQL)
  *
  * Author:   kongbig
  * Data:     2018/1/12.
  */
object IPLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("IPLocation")
    val sc = new SparkContext(conf)

    // 1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
    val ipRulesRDD = sc.textFile("E:\\develop\\spark\\ip.txt").map(line => {
      val fields = line.split("\\|")
      val startNum = fields(2)
      val endNum = fields(3)
      val province = fields(6)
      (startNum, endNum, province)
    })

    // 全部的ip映射规则
    val ipRuleArray = ipRulesRDD.collect()

    // 广播规则
    val ipRulesBroadcast = sc.broadcast(ipRuleArray)

    // 加载要处理的数据
    // 20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/ EmbeddedWB- 14.59  from: http://bsalsa.com/ )|http://show.51.com/main.php|
    val ipsRDD = sc.textFile("E:\\develop\\spark\\access_log").map(line => {
      val fields = line.split("\\|")
      fields(1) // 返回ip
    })

    val result = ipsRDD.map(ip => {
      // 将二进制ip转换成十进制
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesBroadcast.value, ipNum)
      val info = ipRulesBroadcast.value(index) // 根据索引取
      info // 返回结果
    }).map(t => (t._3, 1)).reduceByKey(_ + _)

    // 向MySQL写入数据
    // location_info表：location, count, access
    result.foreachPartition(data2MySQL(_))
    //    println(result.collect().toBuffer)
    sc.stop()
  }

  /**
    * ip转成10进制
    *
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分查找法
    *
    * @param lines (startNum, endNum, province)
    * @param ip
    */
  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      // 属于某个区间范围
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  val data2MySQL = (iterator: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, access_date) VALUES (?, ?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/big_data?useUnicode=true&characterEncoding=utf8", "root", "123")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

}
