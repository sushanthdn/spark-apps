package com.dnanexus.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;


public class SparkTestApp {

  public static void main(String[] args) throws Exception {

    boolean isS3 = "s3".equals(args[2]);
    boolean isReadOnly = "readOnly".equals(args[3]);

    SparkSession spark = SparkSession
      .builder()
      .appName("SparkTestApp")
      .config("DNAxAuth", "azzXtcvlllA")
      .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    if (isS3) {
      jsc.hadoopConfiguration().set("fs.s3a.access.key", args[0]);
      jsc.hadoopConfiguration().set("fs.s3a.secret.key", args[1]);
    }
    
    if (isReadOnly) {
      String url = "dataegress.parquet";
      if (isS3) {
        url = "s3a://zarunbdmfilestest/sparkTest/dataegress.parquet";
      }
      Dataset<Row> parquetFileDF = spark.read().parquet(url);
      parquetFileDF.createOrReplaceTempView("data_egress");
      Dataset<Row> aggDF = spark.sql(
        "SELECT SUM(bytes), user FROM data_egress GROUP BY user");
      aggDF.show();

    } else {
      StructType schema = createSchema();

      // TODO: is this parallelized?
      Dataset<Row> df = spark.read().schema(schema).
        csv("s3a://zarunbdmfilestest/sparkTest/data_egress_20170623.csv");
        //.option("inferSchema", "true")
          //.option("header", "true").load("sample.csv");

      System.out.println("FIRST ROW  in 06/23 " + df.first());

      if (isS3) {
        df.write().mode(SaveMode.Overwrite).
          parquet("s3a://zarunbdmfilestest/sparkTest/dataegress.parquet");
      } else {
        df.write().mode(SaveMode.Overwrite).
          parquet("dataegress.parquet");
      }

      Dataset<Row> df2 = spark.read().schema(schema).
        csv("s3a://zarunbdmfilestest/sparkTest/data_egress_20170624.csv");

      System.out.println("FIRST ROW  in 06/24 " + df2.first());

      if (isS3) {
        df2.write().mode(SaveMode.Append).
          parquet("s3a://zarunbdmfilestest/sparkTest/dataegress.parquet");
      } else {
        df2.write().mode(SaveMode.Append).
          parquet("dataegress.parquet");
      }
    }

    spark.stop();
  }

  private static StructType createSchema() {
    return new StructType().
      add("year", DataTypes.IntegerType).
      add("month", DataTypes.IntegerType).
      add("user", DataTypes.StringType).
      add("project", DataTypes.StringType).
      add("region", DataTypes.StringType).
      add("bytes", DataTypes.LongType);
  }

}
