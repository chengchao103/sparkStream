package com.huawei.hadoop.security.com.huawei.bigdata.spark.examples;

import com.huawei.hadoop.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.sql.*;

/**
 * Created by Administrator on 2017/6/14.
 */
public class ThiftSereverJava {
    public static void main(String[] args)  throws SQLException, ClassNotFoundException, IOException {
        String userPrincipal = "admin";
        String userKeytabPath = "/home/weblogic/admin_1495436187398_keytab/user.keytab";
        String krb5ConfPath = "/home/weblogic/admin_1495436187398_keytab/krb5.conf";
        String ZKServerPrincipal = "zookeeper/hadoop.hadoop.com";

        String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
        String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

        Configuration hadoopConf = new Configuration();
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeytabPath);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal);
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

            String securityConfig = "saslQop=auth-conf;auth=KERBEROS;principal=spark/hadoop.hadoop.com@HADOOP.COM" + ";";


        String HA_CLUSTER_URL = "ha-cluster";
        StringBuilder sb = new StringBuilder("jdbc:hive2://" + HA_CLUSTER_URL + "/default;zk.quorum=30.3.247.193:24002,30.3.247.192:24002,30.3.247.191:24002;" + securityConfig);
        String url = sb.toString();
        System.out.println(url);
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add("CREATE TABLE IF NOT EXISTS CHILD111 (NAME STRING, AGE INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        sqlList.add("LOAD DATA LOCAL INPATH '/home/data' INTO TABLE CHILD111");
//        sqlList.add("SELECT * FROM child2");
  //      sqlList.add("DROP TABLE child");
        executeSql(url, sqlList);
    }

    static void executeSql(String url, ArrayList<String> sqls) throws ClassNotFoundException, SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = DriverManager.getConnection(url);
            for (int i =0 ; i < sqls.size(); i++) {
                String sql = sqls.get(i);
                System.out.println("---- Begin executing sql: " + sql +  " ----");
                statement = connection.prepareStatement(sql);
                ResultSet result = statement.executeQuery();
                ResultSetMetaData resultMetaData = result.getMetaData();
                Integer colNum = resultMetaData.getColumnCount();
                for (int j =1; j <= colNum; j++) {
                    System.out.print(resultMetaData.getColumnLabel(j) + "\t");
                }
                System.out.println();

                while (result.next()) {
                    for (int j =1; j <= colNum; j++){
                        System.out.print(result.getString(j) + "\t");
                    }
                    System.out.println();
                }
                System.out.println("---- Done executing sql: " + sql +  " ----");
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != statement) {
                statement.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
    }
}
