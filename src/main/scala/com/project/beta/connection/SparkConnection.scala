package com.project.beta.connection

import org.apache.spark.sql.SparkSession

class SparkConnection {
  SparkSession
    .builder
    .appName("beta")
    .config("spark.master","local")
    .enableHiveSupport()
    .getOrCreate()
}
