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


public class PhenoCsvToParquet {

  public static void main(String[] args) throws Exception {
    String type = args[1];

    StructType schema = createSchema(type);

    SparkSession spark = SparkSession
      .builder()
      .appName("PhenoCsvToParquet"+type)
      .getOrCreate();

    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


    Dataset<Row> df = spark.read().schema(schema).option("header", true).csv(args[0]);

    df.write().mode(SaveMode.Append).parquet(args[2]);

    spark.stop();
  }

  private static StructType createSchema(String type) {
    if (type.equals("Patients")) {
      return new StructType().
        add("sampleid", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("birthdate", DataTypes.StringType).
        add("deathdate", DataTypes.StringType).
        add("ssn", DataTypes.StringType).
        add("drivers", DataTypes.StringType).
        add("passport", DataTypes.StringType).
        add("prefix", DataTypes.StringType).
        add("first", DataTypes.StringType).
        add("last", DataTypes.StringType).
        add("suffix", DataTypes.StringType).
        add("maiden", DataTypes.StringType).
        add("marital", DataTypes.StringType).
        add("race", DataTypes.StringType).
        add("ethnicity", DataTypes.StringType).
        add("gender", DataTypes.StringType).
        add("birthplace", DataTypes.StringType).
        add("address", DataTypes.StringType);
    } else if (type.equals("Allergies")) {
      return new StructType().
        add("strt", DataTypes.StringType).
        add("stop", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("encounter", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType);
    } else if (type.equals("CarePlans")) {
      return new StructType().
        add("id", DataTypes.StringType).
        add("strt", DataTypes.StringType).
        add("stop", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("encounterid", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType).
        add("reasoncode", DataTypes.StringType).
        add("reasondescription", DataTypes.StringType);
    } else if (type.equals("Conditions")) {
      return new StructType().
        add("strt", DataTypes.StringType).
        add("stop", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("encounter", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType);
    } else if (type.equals("Encounters")) {
      return new StructType().
        add("id", DataTypes.StringType).
        add("dt", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType).
        add("reasoncode", DataTypes.StringType).
        add("reasondescription", DataTypes.StringType);
    } else if (type.equals("Immunizations")) {
      return new StructType().
        add("dt", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("encounterid", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType);
    } else if (type.equals("Medications")) {
      return new StructType().
        add("strt", DataTypes.StringType).
        add("stop", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("encounter", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType).
        add("reasoncode", DataTypes.StringType).
        add("reasondescription", DataTypes.StringType);
    } else if (type.equals("Observations")) {
      return new StructType().
        add("dt", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("encounterid", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType).
        add("value", DataTypes.DoubleType).
        add("units", DataTypes.StringType);
    } else if (type.equals("Procedures")) {
      return new StructType().
        add("dt", DataTypes.StringType).
        add("patientid", DataTypes.StringType).
        add("encounterid", DataTypes.StringType).
        add("code", DataTypes.StringType).
        add("description", DataTypes.StringType).
        add("reasoncode", DataTypes.StringType).
        add("reasondescription", DataTypes.StringType);
    }
    throw new RuntimeException("Unknown type. Known types: Patients, Allergies, CarePlans, Conditions, " +
                               "Encounters, Immunizations, Medications, Observations, Procedures");
  }

}
