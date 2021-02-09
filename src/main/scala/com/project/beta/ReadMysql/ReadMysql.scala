package com.project.beta.ReadMysql

import org.apache.spark.sql.SparkSession

object ReadMysql {
    def main(args: Array[String]): Unit = {


        val spark = SparkSession
          .builder
          .appName("beta")
          .config("spark.master", "local")
          .enableHiveSupport()
          .getOrCreate()

      /*  val query = "(select id,user_name,descr from test_user) " */

        def df = spark.read
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/test")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", "test_user" )
          .option("partitionColumn","id")
          .option("numPartitions","2")
          .option("lowerBound","1")
          .option("upperBound","10")
          .option("fetchsize","10")
          .option("sessionInitStatement",
              "update test_user set create_time =(select CONVERT_TZ (create_time ,'+00:00','-03:00')")
          .option("user", "root")
          .option("password", "0910")
          .load()
          /*
          .limit(40)
          .select("id","create_time","user_name","descr","status","is_visible")
*/



        df.show(50)
/*
        df.write
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/test")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("dbtable", "test_user" )
          .option("user", "root")
          .mode("overwrite")
          .save()

*/

    }

}
