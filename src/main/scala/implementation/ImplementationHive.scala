package implementation

import connection.HiveJDBCConnection

import scala.collection.mutable

/**
  * Created by durga on 02/12/17.
  */
object ImplementationHive {

  def testHiveConnection()=
  {
    val implementationHive=new HiveJDBCConnection("jdbc:hive2://10.1.51.127:10000/diwo","hive","hive")
    implementationHive.init()

    var query ="CREATE EXTERNAL TABLE IF NOT EXISTS sales(" +
      "BRND_CD STRING," +
      "STO_LOC_ID INT," +
      "SHPG_TRXN_ID INT," +
      "SHPG_TRXN_LN_DT DATE," +
      "TRXN_BGN_TM STRING," +
      "SRC_RGST_NBR INT," +
      "ITM_SKU_NUM INT," +
      "SRC_SLS_PRSN_NUM INT," +
      "SRC_SLS_CSHR_NUM INT," +
      "FP_SLS_AMT DECIMAL," +
      "FP_SLS_QTY INT," +
      "CLR_SLS_AMT DECIMAL," +
      "CLR_SLS_QTY INT," +
      "MD_SLS_AMT DECIMAL," +
      "MD_SLS_QTY INT," +
      "GVWY_SLS_AMT DECIMAL," +
      "GVWY_SLS_QTY INT," +
      "NET_SALES DECIMAL," +
      "NET_SLS_QTY INT," +
      "RETURN_SALES DECIMAL," +
      "RETURN_QTY INT," +
      "ANGEL_TRXN char(1)," +
      "WT_UNT_CST_AMT DECIMAL," +
      "CUR_TKT_RTL_UNT_AMT DECIMAL" +
      ") " +
      "COMMENT 'Data about Sales from a diwo database' " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY ',' " +
      "STORED AS TEXTFILE " +
      "location 'hdfs://hdpnode3:8020/apps/hive/warehouse/sales/'";

//    println(query)
    var valid = implementationHive.executeNotSelectQuery(query)
    println(query+" "+valid)

    var query1 ="CREATE EXTERNAL TABLE IF NOT EXISTS product(" +
      "BRND_CD STRING," +
      "STO_LOC_ID INT," +
      "SHPG_TRXN_ID INT," +
      "SHPG_TRXN_LN_DT DATE," +
      "TRXN_BGN_TM TIME," +
      "SRC_RGST_NBR INT," +
      "ITM_SKU_NUM INT," +
      "SRC_SLS_PRSN_NUM INT," +
      "SRC_SLS_CSHR_NUM INT," +
      "FP_SLS_AMT DECIMAL" +
      "FP_SLS_QTY INT," +
      "CLR_SLS_AMT DECIMAL," +
      "CLR_SLS_QTY INT," +
      "MD_SLS_AMT DECIMAL," +
      "MD_SLS_QTY INT," +
      "GVWY_SLS_AMT DECIMAL" +
      "GVWY_SLS_QTY INT," +
      "NET_SALES DECIMAL," +
      "NET_SLS_QTY INT," +
      "RETURN_SALES DECIMAL," +
      "RETURN_QTY INT," +
      "ANGEL_TRXN char(1)" +
      "WT_UNT_CST_AMT DECIMAL," +
      "CUR_TKT_RTL_UNT_AMT DECIMAL" +
      ")" +
      "COMMENT 'Data about Sales from a diwo database'" +
      "ROW FORMAT DELIMITED" +
      "FIELDS TERMINATED BY ','" +
      "STORED AS TEXTFILE" +
      "location '/user/<username>/visdata';"

    var valid2 = implementationHive.executeNotSelectQuery(query1)
    println(query1+" "+valid2)

//   val query3 ="insert into semaine values ('3','dimanche')"
//    valid = implementationHive.executeNotSelectQuery(query3)
//    println(query3+"  "+valid)
//
//   val query4="select * from semaine"
//    var columns=Array("id","name")
//    try {
//      var lines: mutable.MutableList[String] = implementationHive.executeSelectQuery(query4, columns)
//      lines.foreach(println)
//    }
//    catch
//    {
//      case e:Exception => println("Exception "+e.getMessage)
//    }
  }

}
