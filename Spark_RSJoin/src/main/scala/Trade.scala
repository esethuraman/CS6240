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

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;


    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("comm_code_10410.csv")

    var df_export = df.filter(df("flow")==="Export")
    var df_import = df.filter(df("flow")==="Import")

    df_export.join(df_import, Seq("year", "comm_code")).saveAsTextFile("result.csv")

//    getTrade(args(0), args(1), sc)
  }
//
//  // Calculate the number of triangles in the graph dataset
//  def getTrade(input_path: String, output_path: String, conf: SparkContext) = {
//    val MAX_FILTER = 5000
//    val textFile = conf.textFile(input_path)
//    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
//    // Convert each line of csv into pair, filter out the user ids that are greater than MAX_FILTER
//    val edges1 = textFile.map(line => convert(line)).filter(i => i._1 <= MAX_FILTER & i._2 <= MAX_FILTER)
//    // Similar operation as above but it reveresed the pair and filter the ids that are greater than MAX_FILTER
//    val edges2 = textFile.map(line => reverse(line)).filter(i => i._1 <= MAX_FILTER & i._2 <= MAX_FILTER)
//    // Convert each line of csv to pair, filter users greater than MAX_FILTER and create RDD with the relationshp pair as key and 1 as value
//    val edges3 = textFile.map(line => convert(line)).filter(i => i._1 <= MAX_FILTER & i._2 <= MAX_FILTER).map(i => ((i._1, i._2),1))
//    // Join edges1 with edges2 (containing reverse of edges1) on key to retrieve all path 2 pairs then map the values as key pair and 1 as value
//    val path2 = edges1.join(edges2).map(i => ((i._2._1, i._2._2),1))
//    // Join edges3 with path2 obtained from previous step and then count the number of keys
//    val numOfTriangles = path2.join(edges3).count()
//    // Save the result in output file
//    conf.parallelize(Seq("Total number of Triangles", numOfTriangles / 3)).saveAsTextFile(output_path)
//  }
//
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
