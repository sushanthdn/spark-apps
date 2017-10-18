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


public class CsvToParquet {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
      .builder()
      .appName("CsvToParquet")
      .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    StructType schema = createSchema(args[1].split(","));

    Dataset<Row> df = spark.read().schema(schema).csv(args[0]);

    df.write().mode(SaveMode.Append).parquet(args[2]);

    spark.stop();
  }

  private static StructType createSchema(String[] columns) {
    return new StructType().
      add("variantid", DataTypes.StringType).
      add("sampleid", DataTypes.StringType).
      add("genotype", DataTypes.StringType).
      add("dosage", DataTypes.DoubleType).
      add("gl00", DataTypes.DoubleType).
      add("gl01", DataTypes.DoubleType).
      add("gl11", DataTypes.DoubleType);

      /*
    StructType st = new StructType();
    for (String col : columns) {
      System.out.println("Adding col " + col);
      st.add(col, DataTypes.StringType);
    }
    return st;
    */
  }

}
