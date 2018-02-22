package com.kongbig.demo6_custom_sort

import org.apache.spark.{SparkConf, SparkContext}

object OrderContext {
  implicit val GirlOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      // 返回颜值高的
      if (x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue) {
        // 颜值相等就返回年龄小的
        if (x.age > y.age) -1 else 1
      } else -1
    }
  }
}

/**
  * Describe: 自定义排序
  * sort => 先按faceValue，再年龄
  * Author:   kongbig
  * Data:     2018/1/12.
  */
object CustomSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 并行化的方式创建rdd
    val rdd1 = sc.parallelize(List(("zhangsan", 90, 28, 1), ("lisi", 90, 27, 2), ("wangwu", 95, 22, 3)))
    import OrderContext._
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false) // 降序
    println(rdd2.collect().toBuffer)
    sc.stop()
  }

}

/**
  * 第一种方式
  *
  * @param faceValue
  * @param age
  */
//case class Girl(val faceValue: Int, val age: Int) extends Ordered[Girl] with Serializable {
//  override def compare(that: Girl): Int = {
//    if (this.faceValue == that.faceValue) {
//      // 颜值相等就比较年龄
//      that.age - this.age
//    } else {
//      this.faceValue - that.faceValue
//    }
//  }
//}

case class Girl(faceValue: Int, age: Int) extends Serializable