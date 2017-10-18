package com.dnanexus.spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.net.URL;
import java.net.URLClassLoader;


public class ThriftTest {

  public static void main(String[] args) throws Exception {
    Connection con = getDbCon();
    long a = System.currentTimeMillis();
    runQuery(true, con);
    System.out.println("S3 Time taken: " + (System.currentTimeMillis() - a));
    a = System.currentTimeMillis();
    //runQuery(false, con);
    System.out.println("Time taken: " + (System.currentTimeMillis() - a));
  }

  private static void runQuery(boolean isS3, Connection con) throws SQLException {
    long a = System.currentTimeMillis();
    String table = isS3 ? "data_egress_s3" : "data_egress";
		Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT SUM(bytes), user FROM " + table + " GROUP BY user");
    System.out.println("isS3 " + isS3 + " Time for query: " + (System.currentTimeMillis() - a));
    while (rs.next()) {
      System.out.println(rs.getString(2) + "," + rs.getLong(1));
    }
  }

  private static Connection getDbCon() throws SQLException {
    // this is just an example. for production, connection pooling is needed..
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://localhost:10000/bdm", "abhat", "");
    return con;

  }

  private static void printClassPath() {
     System.out.println("CLASSPATH>>>>>");
        ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
        	System.out.println(url.getFile());
        }
     System.out.println("END CLASSPATH>>>>>");
  }


  private static final String DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";
  static {
    //printClassPath();
    try {
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

  }

}
