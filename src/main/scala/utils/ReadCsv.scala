package utils

import org.apache.spark.sql.SparkSession


trait ReadCsv extends SparkConfiguration with PropertiesConfiguration{

  // Get the Employee dataset from input file
  val employeeDataSet = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", ",").load("/"+hdfsPath+"/SparkTest/Employee.csv")

}
