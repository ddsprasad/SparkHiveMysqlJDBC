package index

import implementation.{ImplementationHive, ImplementationMysql}
import org.apache.spark.sql.SparkSession


object SparkHive
{
  def main(args:Array[String]): Unit =
  {
   val spark = SparkSession
      .builder()
       .master("local[*]")
      .appName("Spark Hive Example")
      .getOrCreate()
    ImplementationHive.testHiveConnection()
    //ImplementationMysql.testMysqlConnection()
  }
}