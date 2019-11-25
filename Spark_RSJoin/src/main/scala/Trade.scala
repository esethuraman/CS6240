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

object Trade{

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
      .load(args(0))

    var df_export = df.filter(df("flow")==="Export")
    var df_import = df.filter(df("flow")==="Import")

    df_export.join(df_import, Seq("year", "comm_code")).saveAsTextFile("result.csv")

  }

//  // Split the line in two parts, convert them to int and return them as a pair
//  def convert(line: String): (Int, Int) = {
//    return (line.split(",")(0).toInt, line.split(",")(1).toInt)
//  }
//
//  // Split the line in two parts, convert them to int then reverse the relation and return them as a pair
//  def reverse(line: String): (Int, Int) = {
//    return (line.split(",")(1).toInt, line.split(",")(0).toInt)
//  }

}
