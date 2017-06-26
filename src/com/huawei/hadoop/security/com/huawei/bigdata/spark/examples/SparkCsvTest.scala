package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/14.
  */
object SparkCsvTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("hivetest")
    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)
    import com.databricks.spark.csv._
    val data = sqlContext.read.format("com.databricks.spark.csv")
    .option("header","true")
    .option("inferSchema",true.toString)
    .load("D:\\华为大数据平台\\生产环境\\script\\map_table.csv")
    data.select("SYS_ID","SYS_NAME","SRC_TABLE_NAME").toDF.registerTempTable("FemaleInfoTable")
    val femaleTimeInfo = sqlContext.sql("select SYS_ID,SRC_TABLE_NAME as " +
      "stayTime from FemaleInfoTable ")
    femaleTimeInfo.show()
    // Filter information about female netizens who spend more than 2 hours online.
    val c = femaleTimeInfo.collect().foreach(println)
    sparkContext.stop()
  }
}
