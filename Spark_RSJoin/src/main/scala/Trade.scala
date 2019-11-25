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

object Trade {

  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    if (args.length != 2) {
      logger.error("Usage:\nTrade.Data <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Trade")
    val sc = new SparkContext(conf)

    getTrade(args(0), args(1), sc)
  }

  // Find the cartesian product of two dataset
  def getTrade(input_path: String, output_path: String, conf: SparkContext) = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load(input_path)

    var df_export = df.filter(df("flow") === "Export").filter((df("flow") === "Re-Export")).withColumnRenamed("quantity", "export_quantity")
      .withColumnRenamed("trade_usd","export_trade_usd")
      .withColumnRenamed("quantity_name", "export_quantity_name").withColumnRenamed("flow", "export_flow")
      .withColumnRenamed("category", "expory_category").withColumnRenamed("country_or_area", "export_country_or_area") .withColumnRenamed("category", "expory_category")
      .withColumnRenamed("weight_kg", "export_weight_kg").withColumnRenamed("index", "export_index")

    var df_import = df.filter(df("flow") === "Import").filter((df("flow") === "Re-Import"))

    var df_res = df_export.join(df_import, Seq("year", "comm_code"))
    df_res.write.format("csv").save(output_path)

  }
}