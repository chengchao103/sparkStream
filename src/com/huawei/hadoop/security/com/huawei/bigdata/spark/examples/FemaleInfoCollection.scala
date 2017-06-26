package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/9.
  */
object FemaleInfoCollection {
  def main(args: Array[String]): Unit = {
    val array =  Array("c:\\1.TXT")
    if (array.length < 1) {
      System.err.append("Usage: CollectFemaleInfo <file>")
      System.exit(-1)
    }
    val userPrincipal = "sparkuser"
    val userKeytabPath  = "C:\\Users\\Administrator\\Downloads\\sparkuser_1496996689239_keytab\\user.keytab"
    val userKrb5f = "C:\\Users\\Administrator\\Downloads\\sparkuser_1496996689239_keytab\\krb5.conf"
    val hadoopConf : Configuration = new Configuration()
    LoginUtil.login(userPrincipal,userKeytabPath,userKrb5f,hadoopConf)
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val text = sc.textFile(array(0))
    val data = text.filter(_.contains("female"))

    val femaleData:RDD[(String,Int)] = data.map{line =>

      val t= line.split(',')

      (t(0),t(2).toInt)

    }.reduceByKey(_ + _)
    val result = femaleData.filter(line => line._2 > 0)
    System.out.println("aaaaaaaaaaaaaaaaaaaaaaa")
    result.collect().map(x => x._1 + ',' + x._2).foreach(println)
    System.out.println("bbbbbbbbbbbbbbbbbbbbbbb")
    sc.stop()
  }
}
