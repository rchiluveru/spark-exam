package org.example.sparkExam

import java.io.FileNotFoundException
import org.scalatest.FunSuite
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.util.Properties
import utils.SparkConfiguration

class FirstAssignmentTest extends FunSuite {
  //Get the data set info from properties file
  val prop = new Properties();
  val propFileName = "config.properties";
  val inputStream = getClass.getClassLoader.getResourceAsStream(propFileName);
  if (inputStream != null) {
    prop.load(inputStream);
  } else {
    throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
  }
  val filesPath = prop.getProperty("files.path")
  val returnsDatasetName = prop.getProperty("returns.dataset.name")
  val ordersDatasetName = prop.getProperty("orders.dataset.name")

  //Load the data from data sets
  val inputReturnsFile = Source.fromFile(""+filesPath+"/"+returnsDatasetName+".csv").getLines
  val returnsWithoutHeader = inputReturnsFile.drop(1)
  val returnsData = returnsWithoutHeader.map(x => x.split(",")).map(columnPosition => (columnPosition(0), columnPosition(1))).toList
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  val inputOrdersFile = Source.fromFile(""+filesPath+"/"+ordersDatasetName+".csv").getLines
  val OrdersWithoutHeader = inputOrdersFile.drop(1)
  val OrdersData = OrdersWithoutHeader.map(x => x.split(",")).toList
 // val OrdersData = OrdersWithoutHeader.map(x => x.split(",")).map(columnPosition => (columnPosition(0), columnPosition(1), columnPosition(2), columnPosition(3), columnPosition(4), columnPosition(5), columnPosition(6), columnPosition(7), columnPosition(8), columnPosition(9), columnPosition(10), columnPosition(11), columnPosition(12), columnPosition(13), columnPosition(14), columnPosition(15), columnPosition(16), columnPosition(17), columnPosition(18), columnPosition(19), columnPosition(20))).toList

  test("Check Retuns dataset count should not be empty") {
    val actualResult = returnsData.length
    val expectedResult = 0
    if (actualResult != expectedResult) {
      println("Retuns dataset is not empty")
      assert(1 == 1)
    } else {
      println("Retuns dataset is empty")
      assert(1 == 2)
    }
  }
  test("Check Retuns dataset count should be 296") {
    val actualResult = returnsData.length
    val expectedResult = 296
    if (actualResult == expectedResult) {
      println("Retuns dataset count is matching")
      assert(1 == 1)
    } else {
      println("Retuns dataset count is not matching")
      assert(1 == 2)
    }
  }
  test("Check mocking data of Retuns dataset is correct or not") {
    val actualResult = returnsData.take(2)
    val expectedResult = List(("Yes","CA-2017-153822"),("Yes","CA-2017-129707"))
    if (actualResult == expectedResult) {
      println("mocking data of Retuns dataset is correct")
      assert(1 == 1)
    } else {
      println("mocking data of Retuns dataset is not correct")
      assert(1 == 2)
    }
  }
  test("Check Order_ID column of Retuns dataset has duplicates or not") {
    val actualResult = Source.fromFile(""+filesPath+"/"+returnsDatasetName+".csv").getLines.drop(1).map(x => x.split(",")).map(columnPosition => (columnPosition(1))).toList.distinct.length
    val expectedResult = 296
    if (actualResult == expectedResult) {
      println("Order_ID column of Retuns dataset do not has duplicates")
      assert(1 == 1)
    } else {
      println("Order_ID column of Retuns dataset has duplicates")
      assert(1 == 2)
    }
  }
  test("Check Returned column of Retuns dataset has YES value") {
    val actualResult = Source.fromFile(""+filesPath+"/"+returnsDatasetName+".csv").getLines.drop(1).map(x => x.split(",")).map(columnPosition => (columnPosition(0))).toList.distinct
    val expectedResult = List("Yes")
    if (actualResult == expectedResult) {
      println("Check Returned column of Retuns dataset has YES value")
      assert(1 == 1)
    } else {
      println("Check Returned column of Retuns dataset do not has YES value")
      assert(1 == 2)
    }
  }
  test("Check Orders dataset count should not be empty") {
    val actualResult = OrdersData.length
    val expectedResult = 0
    if (actualResult != expectedResult) {
      println("Orders dataset is not empty")
      assert(1 == 1)
    } else {
      println("Orders dataset is empty")
      assert(1 == 2)
    }
  }
  test("Check Orders dataset count should be 9994") {
    val actualResult = OrdersData.length
    val expectedResult = 9994
    if (actualResult == expectedResult) {
      println("Orders dataset count is matching")
      assert(1 == 1)
    } else {
      println("Orders dataset count is not matching")
      assert(1 == 2)
    }
  }
  test("Check Ordes dataset has duplicate records or not") {
    val actualResult = Source.fromFile(""+filesPath+"/"+ordersDatasetName+".csv").getLines.drop(1).map(x => x.split(",")).toList.distinct.length
    val expectedResult = 9994
    if (actualResult == expectedResult) {
      println("Ordes dataset do not has duplicate records")
      assert(1 == 1)
    } else {
      println("Ordes dataset has duplicate records")
      assert(1 == 2)
    }
  }

}
