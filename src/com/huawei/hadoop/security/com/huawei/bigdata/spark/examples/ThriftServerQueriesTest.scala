package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/6/14.
  */
object ThriftServerQueriesTest {
  def main(args: Array[String]): Unit = {
//    val userPrincipal = "sparkuser"
//    val userKeytabPath  = "C:\\Users\\Administrator\\Downloads\\sparkuser_1496996689239_keytab\\user.keytab"
//    val userKrb5f = "C:\\Users\\Administrator\\Downloads\\sparkuser_1496996689239_keytab\\krb5.conf"
//    val hadoopConf: Configuration  = new Configuration()
//    LoginUtil.login(userPrincipal, userKeytabPath, userKrb5f, hadoopConf)

    val userPrincipal = "sparkuser"
    val userKeytabPath = "/home/weblogic/sparkuser_1496996689239_keytab/user.keytab"
    val krb5ConfPath = "/home/weblogic/sparkuser_1496996689239_keytab/krb5.conf"
    val ZKServerPrincipal = "zookeeper/hadoop.hadoop.com"

    val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME: String = "Client"
    val ZOOKEEPER_SERVER_PRINCIPAL_KEY: String = "zookeeper.server.principal"
    val hadoopConf: Configuration  = new Configuration();
    LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeytabPath)
    LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal)
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

    val HA_CLUSTER_URL = "ha-cluster"
    val sb = new StringBuilder(s"jdbc:hive2://$HA_CLUSTER_URL/default;zk.quorum=30.3.247.193:24002,30.3.247.192:24002,30.3.247.191:24002;saslQop=auth-conf;" +
      s"auth=KERBEROS;principal=spark/hadoop.hadoop.com@HADOOP.COM")
    val url = sb.toString()

    val sqlList = new ArrayBuffer[String]
    sqlList += "CREATE TABLE IF NOT EXISTS CHILD1 (NAME STRING, AGE INT) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
    sqlList += "LOAD DATA LOCAL INPATH '/home/data' INTO TABLE CHILD"
    sqlList += "SELECT * FROM child"
    sqlList += "DROP TABLE child"

    executeSql(url, sqlList.toArray)
  }

  def executeSql(url: String, sqls: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance()
    var connection: Connection = null
    var statement: PreparedStatement = null
    try {
      connection = DriverManager.getConnection(url)
      for (sql <- sqls) {
        println(s"---- Begin executing sql: $sql ----")
        statement = connection.prepareStatement(sql)

        val result = statement.executeQuery()

        val resultMetaData = result.getMetaData
        val colNum = resultMetaData.getColumnCount
        for (i <- 1 to colNum) {
          print(resultMetaData.getColumnLabel(i) + "\t")
        }
        println()

        while (result.next()) {
          for (i <- 1 to colNum) {
            print(result.getString(i) + "\t")
          }
          println()
        }
        println(s"---- Done executing sql: $sql ----")
      }
    } finally {
      if (null != statement) {
        statement.close()
      }

      if (null != connection) {
        connection.close()
      }
    }

  }
}
