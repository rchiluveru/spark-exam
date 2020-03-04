package org.example.sparkExam

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalactic._

object unitTestCases {
  def main(args: Array[String]) {

    (new unitTestCases).execute(color = true, durations = true, shortstacks = true, stats = true)

  }
}
class unitTestCases extends FunSuite {
 // def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark-Exam").getOrCreate()
    val ordersDF = spark.read.format("com.crealytics.spark.excel").option("sheetName", "Orders").option("header", "true").option("inferSchema", "false").load("/user/rajujslns7936/SparkTest/Sample_Superstore.xls")
    val returnsDF = spark.read.format("com.crealytics.spark.excel").option("sheetName", "Returns").option("header", "true").option("inferSchema", "false").load("/user/rajujslns7936/SparkTest/Returns.xls")

    test("Validate that Orders dataset should not be empty") {
      val spark1 = SparkSession.builder().appName("Spark-Exam").getOrCreate()
      val ordersDF1 = spark1.read.format("com.crealytics.spark.excel").option("sheetName", "Orders").option("header", "true").option("inferSchema", "false").load("/user/rajujslns7936/SparkTest/Sample_Superstore.xls")
     // val returnsDF = spark.read.format("com.crealytics.spark.excel").option("sheetName", "Returns").option("header", "true").option("inferSchema", "false").load("/user/rajujslns7936/SparkTest/Returns.xls")

      if (ordersDF1.count != 0) {
        assert(1 == 1)
      } else {
        assert(1 == 2)
      }
    }

    test("Validate that Count of Orders dataset should be 111") {
      if (ordersDF.count == 9994) {
        assert(1 == 1)
      } else {
        assert(1 == 2)
      }
    }

    test("Validate that Row ID column has NULL values") {

      ordersDF.createOrReplaceTempView("table")
      val result = spark.sql("select * from table where Row ID is null")

      if (result.count > 0) {
        assert(1 == 2)
      } else {
        assert(1 == 1)
      }
    }

    test("Validate that Returns dataset should not be empty") {
      if (returnsDF.count != 0) {
        assert(1 == 1)
      } else {
        assert(1 == 2)
      }
    }
    test("Validate that Count of Returns dataset should be 111") {
      if (returnsDF.count == 296) {
        assert(1 == 1)
      } else {
        assert(1 == 2)
      }
    }

 // }
}
