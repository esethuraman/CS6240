package scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType


object Trade_Rep {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nTrade.Data <input dir> <output dir>")
      System.exit(1)
    }

    getTrade(args(0), args(1))
  }

  def getTrade(input_path: String, output_path: String) = {

    val spark = org.apache.spark.sql.SparkSession.builder
//      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(input_path)

    //  add this for filtering with re-export: || df("flow") === "Re-Export") and || df("flow") === "Re-Import"

    // filter on export and rename the columns
    var df_export = df.filter(df("weight_kg") =!= "" && (df("flow") === "Export" || df("flow") === "Re-Export"))
      .withColumnRenamed("quantity", "exportQuantity").withColumnRenamed("trade_usd","exportTradeUsd")
      .withColumnRenamed("quantity_name", "exportQuantityName").withColumnRenamed("flow", "exportFlow")
      .withColumnRenamed("category", "exportCategory").withColumnRenamed("country_or_area", "exportCountry")
      .withColumnRenamed("commodity", "exportCommodity").withColumn("exportWeight", df("weight_kg").cast(FloatType)).drop("weight_kg")

    // filter on import and rename the columns
    var df_import = df.filter(df("weight_kg") =!= "" && (df("flow") === "Import" || df("flow") === "Re-Import"))
      .withColumnRenamed("quantity", "importQuantity").withColumnRenamed("trade_usd","importTradeUsd")
      .withColumnRenamed("quantity_name", "importQuantityName").withColumnRenamed("flow", "importFlow")
      .withColumnRenamed("category", "importCategory").withColumnRenamed("country_or_area", "importCountry")
      .withColumnRenamed("commodity", "importCommodity").withColumn("importWeight", df("weight_kg").cast(FloatType)).drop("weight_kg")

    // join on comm_code and year and then sort by "exportWeight" in desc order
    var df_res = df_export.join(broadcast(df_import), Seq("comm_code", "year")).sort(desc("exportWeight"), desc("importWeight")).select("comm_code", "year", "exportCountry", "exportWeight", "importCountry", "importWeight")

    // save the result in csv format
    df_res.write.format("csv").save(output_path)
  }
}

