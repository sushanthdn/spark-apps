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


public class SparkTestAppEMR {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("SparkTestAppEMR")
      .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    StructType schema = createSchema();

    if (args[1].equals("s3")) {
      Dataset<Row> df = spark.read().schema(schema).
          csv("s3a://arunsparktest/inputFiles/" + args[0]);

      //TODO: uncomment 
      //df.write().mode(SaveMode.Append).
      //parquet("s3a://arunsparktest/awsbilling.parquet");
    } else {
      Dataset<Row> df = spark.read().schema(schema).csv(args[0]);

      df.write().mode(SaveMode.Append).
      parquet("awsbilling.parquet");
    }

    spark.stop();
  }

  private static StructType createSchema() {
    return new StructType().
      add("invoiceid", DataTypes.StringType).
      add("payeraccountid", DataTypes.StringType).
      add("linkedaccountid", DataTypes.StringType).
      add("recordtype", DataTypes.StringType).
      add("recordid", DataTypes.StringType).
      add("productname", DataTypes.StringType).
      add("rateid", DataTypes.StringType).
      add("subscriptionid", DataTypes.StringType).
      add("pricingplanid", DataTypes.StringType).
      add("usagetype", DataTypes.StringType).
      add("operation", DataTypes.StringType).
      add("availabilityzone", DataTypes.StringType).
      add("reservedinstance", DataTypes.StringType).
      add("itemdescription", DataTypes.StringType).
      add("usagestartDate", DataTypes.StringType).
      add("usageendDate", DataTypes.StringType).
      /*
      add("UsageQuantity", DataTypes.StringType).
      add("BlendedRate", DataTypes.StringType).
      add("BlendedCost", DataTypes.StringType).
      add("UnBlendedRate", DataTypes.StringType).
      add("UnBlendedCost", DataTypes.StringType).
      */
      add("usagequantity", DataTypes.DoubleType).
      add("blendedrate", DataTypes.DoubleType).
      add("blendedcost", DataTypes.DoubleType).
      add("unblendedrate", DataTypes.DoubleType).
      add("unblendedcost", DataTypes.DoubleType).
      add("resourceid", DataTypes.StringType).
      add("username", DataTypes.StringType).
      add("usercmidentity", DataTypes.StringType).
      add("username2", DataTypes.StringType);
  }

}
