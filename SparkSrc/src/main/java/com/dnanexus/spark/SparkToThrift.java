package com.dnanexus.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkToThrift {

  public static void main(String[] args) throws Exception {

    connectToThrift(args);
    //connectToRedshift(args);
  }

  private static void connectToThrift(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("SparkToThrift")
      .config("DNAxAuth", "azzXtcvlllA")
      .getOrCreate();

    Properties connectionProperties = new Properties();
    connectionProperties.put("user", args[0]);
    connectionProperties.put("password", args[0]);
    connectionProperties.put("driver", "org.apache.hive.jdbc.HiveDriver");

    Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:hive2://" + args[1] + ":" + args[2], 
        //"data_egress", connectionProperties);
        "(SELECT * FROM bdm.data_egress_dnax WHERE user LIKE '%-abhat')", connectionProperties);
        //"(SELECT * FROM bdm.data_egress_s3 WHERE project = 'project-F4gKxyQ014XGJ3zK5bBQy0g4')", connectionProperties);
        //"(select count(*), availabilityzone from bdm.aws_billing_s3 group by availabilityzone limit 100)", 
        //connectionProperties);


    jdbcDF.printSchema();
    Row[] rows = (Row[])jdbcDF.head(100);
    System.out.println("First 100 rows " + jdbcDF.head(100));
    for (Row row : rows) System.out.println(row);
    //System.out.println("First 100 rows " + jdbcDF.head(100));

    //jdbcDF.createOrReplaceTempView("data_egress_arun");

    //jdbcDF.registerTempTable("data_egress_arun");
    //Dataset<Row> aggDF = spark.sql("SELECT SUM(bytes) FROM data_egress_arun"); 
    //aggDF.show();

    spark.stop();

  }


  private static void connectToRedshift(String[] args) throws Exception {
    SparkSession spark = SparkSession
      .builder()
      .appName("SparkToThrift")
      .getOrCreate();

    Properties connectionProperties = new Properties();
    connectionProperties.put("user", args[0]);
    connectionProperties.put("password", args[1]);
    connectionProperties.put("driver", "com.amazon.redshift.jdbc41.Driver");

    Dataset<Row> jdbcDF = spark.read().jdbc("jdbc:redshift://" + args[2] + ":" + args[3], 
        "(SELECT * FROM cdm_data_egress WHERE cdm_billto LIKE '%-abhat')", connectionProperties);
        //"(select count(*), availabilityzone from bdm.aws_billing_s3 group by availabilityzone limit 100)", 
        //connectionProperties);


    jdbcDF.printSchema();
    jdbcDF.registerTempTable("data_egress_arun");
    Row[] rows = (Row[])jdbcDF.head(100);
    System.out.println("First 100 rows " + jdbcDF.head(100));
    for (Row row : rows) System.out.println(row);
    //System.out.println("First 100 rows " + jdbcDF.head(100));

    //jdbcDF.createOrReplaceTempView("data_egress_arun");

    Dataset<Row> aggDF = spark.sql("SELECT SUM(cdm_egressbytes) FROM data_egress_arun"); 
    aggDF.show();

    spark.stop();
  }
}
