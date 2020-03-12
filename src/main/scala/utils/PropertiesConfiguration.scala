package utils

import java.io.FileNotFoundException
import java.util.Properties

trait PropertiesConfiguration {
  val prop = new Properties();
  val propFileName = "config.properties";
  val inputStream = getClass.getClassLoader.getResourceAsStream(propFileName);
  if (inputStream != null) {
    prop.load(inputStream);
  } else {
    throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
  }
  //Get the properties values from SparkConfiguration trait
  val hdfsPath = prop.getProperty("hdfs.path")
  val ordersDatasetName = prop.getProperty("orders.dataset.name")
  val returnsDatasetName = prop.getProperty("returns.dataset.name")
  val firstResultExcelName = prop.getProperty("first.result.excel.name")
  val secondResultExcelName = prop.getProperty("second.result.excel.name")
  val firstResultCsvName = prop.getProperty("second.assignment.result.one")
  val secondResultCsvName = prop.getProperty("second.assignment.result.two")

}
