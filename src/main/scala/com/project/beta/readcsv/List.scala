package com.project.beta.readcsv
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object List {
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
      .option("mode","DROPMALFORMED")
      .load(args(0))


    df.write
      .format("csv")
      .mode("overwrite")
      .partitionBy("team")
      .save("/apps/hive/external/default/dataex")


    val fd = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fx = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fk = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs.listStatus(new Path("/apps/hive/external/default/dataex"))
      .filter(_.isDirectory)
      .map(_.getPath)
      .foreach(fs =>
        fx.listStatus(new Path(s"$fs"))
          .filter(_.isFile)
          .map(_.getPath)
          .foreach( fx =>
           {def  df = spark.read
              .format("csv")
              .option("header","false")
              .option("mode","DROPMALFORMED")
              .load(s"$fx")
              .toDF("Name", "Position", "Height", "Weight", "Age")




             df.write
               .format("csv")
               .mode("append")
               .partitionBy("position")
               .save(s"$fs")

             fd.delete(new Path(s"$fx"), true)

           }
    )
      )


    spark.sql("drop table if exists default.dataex")

    spark.sql(
      s""" CREATE EXTERNAL TABLE IF NOT EXISTS
         | dataex (name String, height Int, weight Int, age Float)
         | Comment 'Table des joueurs'
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY ','
         | STORED AS TEXTFILE
         | PARTITIONED BY (team string, position string)
         | location '/apps/hive/external/default/dataex/'
         |""".stripMargin

    )
    fk.listStatus(new Path("/apps/hive/external/default/dataex"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("team=",""))
      .foreach(fk =>
        {

    fx.listStatus(new Path(s"/apps/hive/external/default/dataex/team=$fk"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("position=", ""))
      .foreach(fx =>
        spark.sql(s"""alter table dataex add if not exists partition(team='$fk',position='$fx')"""))


        })

    spark.sql("SELECT * FROM DataEx").show()




}}
