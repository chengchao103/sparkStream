package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/9.
  */
case class FemaleInfo(name:String,gender:String,stayTime:Int)
object SparkOnHiveTest {
  def main(args: Array[String]): Unit = {
    val userPrincipal = "sparkuser"
    val userKeytabPath  = "C:\\Users\\Administrator\\Downloads\\sparkuser_1496996689239_keytab\\user.keytab"
    val userKrb5f = "C:\\Users\\Administrator\\Downloads\\sparkuser_1496996689239_keytab\\krb5.conf"
    val hadoopConf: Configuration  = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, userKrb5f, hadoopConf)
    val sparkConf = new SparkConf().setAppName("hivetest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    sc.textFile("c:\\1.TXT").map(_.split(","))
      .map(p => FemaleInfo(p(0), p(1), p(2).trim.toInt))
      .toDF.registerTempTable("FemaleInfoTable")

    val femaleTimeInfo = sqlContext.sql("select name,sum(stayTime) as " +
      "stayTime from FemaleInfoTable where gender = 'female' group by name")

    // Filter information about female netizens who spend more than 2 hours online.
    val c = femaleTimeInfo.filter("stayTime >= 0").collect().foreach(println)
    sc.stop()
  }
}
