package com.project.beta.readcsv

import org.apache.spark.sql.SparkSession

object CsvMysql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("beta")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

 def df = spark.read
   .format("csv")
   .option("header","true")
   .option("mode","dropmalformed")
   .load(args(0))

    df.show()

    df.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/test")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","test_data")
      .mode("overwrite")
      .save()



  }

}
