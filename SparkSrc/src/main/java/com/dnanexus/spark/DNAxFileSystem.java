package com.dnanexus.spark;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hive.shims.Utils;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.RuntimeConfig;

public class DNAxFileSystem extends S3AFileSystem {

  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    System.out.println("*** URI is " + name);
    //System.out.println("   CONFIG IS:\n");
    //Configuration.dumpConfiguration(conf, new OutputStreamWriter(System.out));
    try {
      System.out.println("**** DNAxFile USERname from Utils is  " + Utils.getUGI().getUserName());
      /*System.out.println("*** CHECKING auth code");

      SparkSession spark = SparkSession.builder().getOrCreate(); // maybe getActiveSession in 2.2.0
      RuntimeConfig rconf = spark.conf();
      System.out.println("*** Auth code is " + rconf.get("DNAxAuth"));*/

    } catch (Exception x) {
      System.out.println("**** * DNAxFile Exception in Utils..");
      x.printStackTrace();
    }
    System.out.println("************************");
  }

  public DNAxFileSystem() {
    super();
  }

  public String getScheme() {
    return "dnax";
  }

}
