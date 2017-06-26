package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/15.
  */
object SparkRDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Sparktest")
    val sc = new SparkContext(conf)
    val z = sc.parallelize(List(1,2,3,4,5,6), 2)
    //val a = z.mapPartitionsWithIndex(myfunc).collect
    val a = z.aggregate(20)(math.max(_,_), _ + _)
    println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    //println(a.mkString(","))
    println(a)
    println("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    val z1 = sc.parallelize(List("a","b","c","d","e","f"),2)
    print(z1.aggregate("x")(_ + _, _+_))
    val a1 = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a1.map((_, "b"))
    val c = a1.map((_, "c"))
    println(b.cogroup(c).collect.mkString(","))

    val a2 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val b2 = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
    val c2 = b2.zip(a2)
    val d2 = c2.combineByKey(List(_), (x:List[String], y:String) => y :: x, (x:List[String], y:List[String]) => x ::: y)
    println(d2.collect.mkString(","))
  }

  def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
  }
}
