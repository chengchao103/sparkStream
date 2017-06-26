package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by Administrator on 2017/6/16.
  */
object SQLContextTest {
  def main(args: Array[String]): Unit = {
    println("ddddd")
    val conf = new SparkConf().setAppName("hivefuckthroughspark")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new HiveContext(sparkContext)
    //sqlContext.sql("use odshive")
    sqlContext.sql("select * from xxxxx").collect().foreach(println)
    sparkContext.stop()

  }
}
