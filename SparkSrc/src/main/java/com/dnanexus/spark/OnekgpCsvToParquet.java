package com.dnanexus.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.List;


public class OnekgpCsvToParquet {

  public static void main(String[] args) throws Exception {
    String type = args[1];

    StructType schema = createSchema(type);

    SparkSession spark = SparkSession
      .builder()
      .appName("OnekgpCsvToParquet"+type)
      .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


    Dataset<Row> df = spark.read().schema(schema).option("header", true).csv(args[0]);

    DataFrameWriter<Row> dw = df.write().mode(SaveMode.Append);
    if (type.equals("VariantSamples")) {
      dw.partitionBy("sampleid");
    }
    dw.parquet(args[2]);

    spark.stop();
  }

  private static StructType createSchema(String type) {
    if (type.equals("VariantSamples")) {
      return new StructType().
        add("variantid", DataTypes.StringType).
        add("sampleid", DataTypes.StringType).
        add("genotype", DataTypes.StringType);
    } else if (type.equals("Samples")) {
      return new StructType().
        add("id", DataTypes.StringType).
        add("name", DataTypes.StringType).
        add("ethnicity", DataTypes.StringType);
    } else if (type.equals("Variants")) {
      return new StructType().
        add("id", DataTypes.StringType).
        add("chr", DataTypes.IntegerType).
        add("start", DataTypes.IntegerType).
        add("stop", DataTypes.IntegerType).
        add("ref", DataTypes.StringType).
        add("alt", DataTypes.StringType).
        add("rsid", DataTypes.StringType).
        add("qual", DataTypes.StringType);
    }
    throw new RuntimeException("Unknown type. Known types: VariantSamples, Samples, Variants");
  }

}
