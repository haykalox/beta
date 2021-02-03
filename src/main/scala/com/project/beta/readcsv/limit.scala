package com.project.beta.readcsv

import org.apache.spark.sql.SparkSession



object limit {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("beta")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()


    val df = spark.read
      .format("csv")
      .option("header","true")
      .option("mode","DROPMALFORMED")
      .load(args(0))
      .limit(50)
      .select("name","team")



  }

}
