package index

/**
  * Created by prasad on 12/12/2017.
  */

import org.apache.spark.{SparkConf, SparkContext}
import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive._

import java.util.Properties
import org.apache.spark.sql.SparkSession

object AggregateApp extends App  {

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  //Initialising Spark Context
  val appName = "Sales"

  val conf = new SparkConf()
  conf.setAppName(appName)
  conf.setMaster("local[*]")
  conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.debug","maxToStringFields")
  conf.set("spark.sql.crossJoin.enabled", "true")
  conf.set("spark.kryoserializer.buffer.max","768m")
  conf.set("spark.sql.codegen","true")
  conf.set("spark.executor.memory", "3g")

  val spark = SparkSession.builder()
    .master("local[*]")
    //      .master("local[*]")
    .config("spark.sql.warehouse.dir", "E:\\Vdiwo\\sparkWarehouse")
    .config("hive.metastore.warehouse.dir", "E:\\Vdiwo\\sparkWarehouse")
    // .enableHiveSupport()
    .config(conf)
    .getOrCreate()

  //Initializing SqlContext
  //  val sqlContext = new org.apache.spark.sql.SQLContext(spark)
  import spark.sqlContext.implicits._
  // val hiveContext = new HiveContext(spark.sqlContext)

  //import hiveContext.implicits._

  //read data perform some transformation


  case class Sales(BRND_CD: String, STO_LOC_ID: String, SHPG_TRXN_ID: String, SHPG_TRXN_LN_DT: String,
                   TRXN_BGN_TM:String, SRC_RGST_NBR: String, ITM_SKU_NUM: String, SRC_SLS_PRSN_NUM: String,
                   SRC_SLS_CSHR_NUM:String, FP_SLS_AMT: String, FP_SLS_QTY: String, CLR_SLS_AMT: String,
                   CLR_SLS_QTY:String, MD_SLS_AMT: String, MD_SLS_QTY: String, GVWY_SLS_AMT: String,
                   GVWY_SLS_QTY:String, NET_SALES: String, NET_SLS_QTY: String, RETURN_SALES: String,
                   RETURN_QTY:String, ANGEL_TRXN: String, WT_UNT_CST_AMT: String, CUR_TKT_RTL_UNT_AMT:String)

  val salesrdd0 = spark.sparkContext.textFile("D:\\Diwo\\Sales_data_August2017.zip\\2017-08-05\\")
  //  val salesrdd0 = spark.sparkContext.files("D:\\Diwosales\\Sales_data_August2017\\2017-08-01\\_2017-08-01_")
  //    .textFile("D:\\Diwosales\\Sales_data_August2017\\2017-08-01\\_2017-08-01_")
  //val salesrdd = spark.sparkContext .textFile(slaesfiles)
  //.toDF().count()

  val Sales_df = salesrdd0.map(_.replace("\"", "").split(",")).map(
    x => {
      Sales(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),
        x(15),x(16),x(17),x(18),x(19),x(20),x(21),x(22),x(23)

      )
    }
  ).toDF().show(10,false)

  //val df =  spark.sparkContext.textFile(slaesfiles)
  val Products_df = spark.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load("D:\\Diwo\\DIWO_Product_20171015.txt.gz\\DIWO_Product_20171015.txt")
  Products_df.show(10, false)

  val selseted_prod_df = Products_df.select($"ITM_SKU_NUM" , $"CHC_DESC", $"ITM_STY_DESC", $"FABRICFAMILY",  $"COLORFAMILY")
  selseted_prod_df.show(10, false)

  //  //with save mode
  //  selseted_prod_df.write.mode("append").options(Map("table" -> finalTable,
  //            "cluster" -> "10.1.51.96")).saveAsTable(finalTable)
  //         }
  // show all the tables
  // spark.catalog.listTables.show()

  // save table to hive
  selseted_prod_df.write.saveAsTable("sales_saved")

}
