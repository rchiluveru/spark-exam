package org.example.sparkExam

import utils.{PropertiesConfiguration, ReadCsv, SparkConfiguration}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object EmployeeSparkAssignment extends SparkConfiguration with App with ReadCsv with PropertiesConfiguration{

  try {
    if (employeeDataSet.count != 0) {
      // Print first 5 records in Sorted order by employee id
      val windowSpec = Window.orderBy("emp_id")
      val orderDataSet = employeeDataSet.withColumn("Rank", dense_rank().over(windowSpec))
      val resultOrderDataSet = orderDataSet.filter("Rank <= 5").select("emp_id","emp_name","job_name","salary","dep_id")
      resultOrderDataSet.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("/"+hdfsPath+"/SparkTest/Results/"+firstResultCsvName+".csv")

      //Calculate the average salary by dept
      val windowSpecAgg = Window.partitionBy("dep_id")
      val avgSalaryDataSet = employeeDataSet.withColumn("avg_salary", avg(col("salary")).over(windowSpecAgg))
      val resultAvgDataSet = avgSalaryDataSet.select("dep_id","avg_salary").distinct()
      resultAvgDataSet.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("/"+hdfsPath+"/SparkTest/Results/"+secondResultCsvName+".csv")
    } else {
      println("Employee dataset is empty")
    }
  }
  catch {
    case _: Throwable => println("something really weird happened")
  } finally {
    println("Spark Job is completed successfully")
    spark.close()
  }

}
