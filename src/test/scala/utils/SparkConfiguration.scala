package utils

import org.apache.spark.sql.SparkSession

trait SparkConfiguration {

  val spark = SparkSession.builder().appName("Spark-Exam-Test").getOrCreate()
}
