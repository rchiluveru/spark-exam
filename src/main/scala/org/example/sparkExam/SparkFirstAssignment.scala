package org.example.sparkExam

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.SparkConfiguration
import java.io.FileNotFoundException
import java.util.Properties

object SparkFirstAssignment extends SparkConfiguration with App {
  //Get the data set info from properties file
  val prop = new Properties();
  val propFileName = "config.properties";
  val inputStream = getClass.getClassLoader.getResourceAsStream(propFileName);
  if (inputStream != null) {
    prop.load(inputStream);
  } else {
    throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
  }
  val hdfsPath = prop.getProperty("hdfs.path")
  val ordersDatasetName = prop.getProperty("orders.dataset.name")
  val returnsDatasetName = prop.getProperty("returns.dataset.name")
  val firstResultExcelName = prop.getProperty("first.result.excel.name")
  val secondResultExcelName = prop.getProperty("second.result.excel.name")

  try {
    //1. Read superstore sales data from Excel sheet
    val ordersExcel = spark.read.format("com.crealytics.spark.excel").option("sheetName", "Orders").option("header", "true").option("inferSchema", "false").load("/"+hdfsPath+"/SparkTest/"+ordersDatasetName+".xls")
    val ordersDF = ordersExcel.select(
      ordersExcel.col("Row ID").cast("integer"),
      ordersExcel.col("Order ID"),
      ordersExcel.col("Order Date").cast("timestamp"),
      ordersExcel.col("Ship Date").cast("timestamp"),
      ordersExcel.col("Ship Mode"),
      ordersExcel.col("Customer ID"),
      ordersExcel.col("Customer Name"),
      ordersExcel.col("Segment"),
      ordersExcel.col("Country"),
      ordersExcel.col("City"),
      ordersExcel.col("State"),
      ordersExcel.col("Postal Code").cast("integer"),
      ordersExcel.col("Region"),
      ordersExcel.col("Product ID"),
      ordersExcel.col("Category"),
      ordersExcel.col("Sub-Category"),
      ordersExcel.col("Product Name"),
      ordersExcel.col("Sales").cast("double"),
      ordersExcel.col("Quantity").cast("integer"),
      ordersExcel.col("Discount").cast("double"),
      ordersExcel.col("Profit").cast("double")
    )
    val returnsDF = spark.read.format("com.crealytics.spark.excel").option("sheetName", "Returns").option("header", "true").option("inferSchema", "false").load("/"+hdfsPath+"/SparkTest/"+returnsDatasetName+".xls")

    if (ordersDF.count != 0 && returnsDF.count != 0) {
      //2. Find customers who returned most items after delivery in descending order and write result in excel file.
      val joinDF = ordersDF.join(returnsDF, "Order ID")
      val windowAgg = Window.partitionBy("Customer ID")
      import spark.implicits._
      val aggJoinDF = joinDF.withColumn("MostReturnedItems", sum(col("Quantity")).over(windowAgg)).orderBy($"MostReturnedItems".desc)
      val finalResultDF = aggJoinDF.select("Customer ID", "Customer Name", "MostReturnedItems").distinct()
      finalResultDF.write.format("com.crealytics.spark.excel").option("sheetName", "Sheet1").option("header", "true").mode("overwrite").save("/"+hdfsPath+"/SparkTest/Results/"+firstResultExcelName+".xlsx")

      //3.Calculate state wise total and average sales and profits write result in excel file.
      val windowSpecAgg = Window.partitionBy("State")
      val aggDF = ordersExcel.withColumn("total_sales", sum(col("Sales")).over(windowSpecAgg)).withColumn("avg_sales", avg(col("Sales")).over(windowSpecAgg)).withColumn("total_profits", sum(col("Profit")).over(windowSpecAgg)).withColumn("avg_profits", avg(col("Profit")).over(windowSpecAgg))
      val aggResultDF = aggDF.select("State", "total_sales", "avg_sales", "total_profits", "avg_profits").distinct()
      aggResultDF.write.format("com.crealytics.spark.excel").option("sheetName", "Sheet1").option("header", "true").mode("overwrite").save("/"+hdfsPath+"/SparkTest/Results/"+secondResultExcelName+".xlsx")
    } else {
      println("Orders or Returns dataset is empty")
    }
  }
  catch {
    case _: Throwable => println("something really weird happened")
  } finally {
    println("Spark Job is completed successfully")
    spark.close()
  }

}