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


public class JobDataParquet {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("JobDataParquet")
      .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    StructType schema = createSchema();

    Dataset<Row> df = spark.read().schema(schema).csv(args[0]);

    df.write().mode(SaveMode.Append).parquet("jobdata.parquet");

    spark.stop();
  }

  private static StructType createSchema() {
    return new StructType().
      add("rundate", DataTypes.StringType).
      add("jobid", DataTypes.StringType).
      add("rootexec", DataTypes.StringType).
      add("instanceid", DataTypes.StringType).
      add("instancetype", DataTypes.StringType).
      add("jobtype", DataTypes.StringType).
      add("starttime", DataTypes.StringType).
      add("stoptime", DataTypes.StringType).
      add("executablename", DataTypes.StringType);
  }

}
