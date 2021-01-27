package com.project.beta.readcsv
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object ReadCsv {
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

    df.show()

    //internal
    df.write
      .mode("overwrite")
      .saveAsTable("Data")


    //external
    df.write
      .format("csv")
      .mode("overwrite")
      .partitionBy("team")
      .save("/apps/hive/external/default/dataex")

    // drop table
    spark.sql("drop table if exists default.dataex")

    spark.sql(
      s""" CREATE EXTERNAL TABLE IF NOT EXISTS
         | dataex (Name String, Position String, Height Int, Weight Int, Age Float)
         | Comment 'Table des joueurs'
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY ','
         | STORED AS TEXTFILE
         | PARTITIONED BY (team string)
         | location '/apps/hive/external/default/dataex/team'
         |""".stripMargin

    )

    //lister tous les dossiers dans le,location (fileSystem + listStatus + isDirectory)
    //recuprer juste le nom du dossier (team=XXX)
    //créer la requete de add partition

    //println(s"""""")
   // val tableName="dataex"
  //  list.foreach(f=> {
  //    spark.sql(s"""alter table datex add partitoin(team='$f')""")
  //  })
   val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.listStatus(new Path("/apps/hive/external/default/dataex"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("team=",""))
      .foreach(fs =>
       spark.sql(s"""alter table dataex add if not exists partition(team='$fs')"""))
    val fx = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fx.listStatus(new Path(s"$fs"))
      .filter(_.isDirectory)
      .map(_.getPath.getName.replaceFirst("position=",""))
      .foreach(fs =>
        spark.sql(s"""alter table dataex add if not exists partition(team='$fs')"""))


    spark.sql("SELECT * FROM DataEx").show()

}}
