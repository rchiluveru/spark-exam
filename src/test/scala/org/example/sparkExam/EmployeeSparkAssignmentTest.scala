package org.example.sparkExam

import org.scalatest.FunSuite
import scala.io.Source

class EmployeeSparkAssignmentTest extends FunSuite {

  //Load the data from data sets
  val inputEmployeeFile = Source.fromFile("src/test/resources/Employee.csv").getLines
  val employeeWithoutHeader = inputEmployeeFile.drop(1)
  val employeeData = employeeWithoutHeader.map(x => x.split(",")).map(columnPosition => (columnPosition(0).toInt, columnPosition(1).toString, columnPosition(2).toString, columnPosition(3).toInt, columnPosition(4).toInt)).toList

  test("Check Employee dataset count should not be empty") {
    val actualResult = employeeData.length
    val expectedResult = 0
    if (actualResult != expectedResult) {
      println("Employee dataset is not empty")
      assert(1 == 1)
    } else {
      println("Employee dataset is empty")
      assert(1 == 2)
    }
  }
  test("Check Employee dataset count should be 14") {
    val actualResult = employeeData.length
    val expectedResult = 14
    if (actualResult == expectedResult) {
      println("Employee dataset count is matching")
      assert(1 == 1)
    } else {
      println("Employee dataset count is not matching")
      assert(1 == 2)
    }
  }
  test("Check mocking data of Employee dataset is correct or not") {
    val actualResult = employeeData.take(1)
    val expectedResult = List((68319,"KAYLING ","PRESIDENT",6000,1001))
    if (actualResult == expectedResult) {
      println("mocking data of Employee dataset is correct")
      assert(1 == 1)
    } else {
      println("mocking data of Employee dataset is not correct")
      assert(1 == 2)
    }
  }
  test("Check emp_id column of Employee dataset has duplicates or not") {
    val actualResult = Source.fromFile("src/test/resources/Employee.csv").getLines.drop(1).map(x => x.split(",")).map(columnPosition => (columnPosition(0))).toList.distinct.length
    val expectedResult = 14
    if (actualResult == expectedResult) {
      println("emp_id column of Employee dataset do not has duplicates")
      assert(1 == 1)
    } else {
      println("emp_id column of Employee dataset has duplicates")
      assert(1 == 2)
    }
  }
  test("Check maximum salary from Employee dataset") {
    val actualResult = Source.fromFile("src/test/resources/Employee.csv").getLines.drop(1).map(x => x.split(",")).map(columnPosition => (columnPosition(3).toInt)).toList.distinct.max
    val expectedResult = 6000
    if (actualResult == expectedResult) {
      println("Maximum salary is as expected")
      assert(1 == 1)
    } else {
      println("Maximum salary is not as expected")
      assert(1 == 2)
    }
  }
  test("Check minimum salary from Employee dataset") {
    val actualResult = Source.fromFile("src/test/resources/Employee.csv").getLines.drop(1).map(x => x.split(",")).map(columnPosition => (columnPosition(3).toInt)).toList.distinct.min
    val expectedResult = 900
    if (actualResult == expectedResult) {
      println("minimum salary is as expected")
      assert(1 == 1)
    } else {
      println("minimum salary is not as expected")
      assert(1 == 2)
    }
  }


}
