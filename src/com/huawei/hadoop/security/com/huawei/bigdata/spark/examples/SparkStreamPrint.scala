package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples
import kafka.serializer.StringDecoder
import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
/**
  * Created by Administrator on 2017/6/13.
  */
object SparkStreamPrint {
  val logger = Logger.getLogger(SparkStreamPrint.getClass)
  def main(args: Array[String]): Unit = {
    val userPrincipal = "cdrcb"
    val hadoopConf :Configuration = new Configuration()
    val batchTime="5"
    val windowTime="5"
    val topics="test12345"
    val brokers="30.3.247.191:21005,30.3.247.192:21005,30.3.247.193:21005,30.3.247.194:21005"
    val batchDuration =  Seconds(batchTime.toInt)
    val windowDuration = Seconds(windowTime.toInt)
    val sparkConf = new SparkConf()
    sparkConf.setAppName("sparkstreamtest1122222")
    val ssc = new StreamingContext(sparkConf,batchDuration)
   ssc.checkpoint("checkPoint")
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    logger.info("main : kafkaParams = " + kafkaParams + "   topicsSet= " + topicSet)
//    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, topicSet).map(_._2)
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
      .map(x=>(x._2))
      .filter(_!=null)
    val records = lines.map(getRecord)
    logger.info("fuck="+records)
    val femaleRecords = records.filter(_._2 == "female")
    .map(x=>(x._1,x._3))
    val aggregateRecords = femaleRecords.reduceByKeyAndWindow(_ + _, _ - _, windowDuration)
    aggregateRecords.filter(_._2 > 0.1 * windowTime.toInt).print()
    ssc.start()
    ssc.awaitTermination()
  }

  def getRecord(line:String):(String,String,Int) = {
    val elems = line.split(",")
    val name = elems(0)
    val sexy = elems(1)
    val time = elems(2).toInt
    (name,sexy,time)
  }
}
