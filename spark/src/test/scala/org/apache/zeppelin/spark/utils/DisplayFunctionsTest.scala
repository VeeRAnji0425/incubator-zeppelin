package org.apache.zeppelin.spark.utils

import java.io.ByteArrayOutputStream

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest._
import org.scalatest.{BeforeAndAfter}

case class Person(login : String, name: String, age: Int)

class DisplayFunctionsTest extends FlatSpec with BeforeAndAfter with BeforeAndAfterEach with Matchers {
  var sc: SparkContext = null
  var testRDDTuples: RDD[(String,String,Int)]  = null
  var testRDDPersons: RDD[Person]  = null
  var stream: ByteArrayOutputStream = null
  
  before {
    val sparkConf: SparkConf = new SparkConf(true)
      .setAppName("test-DisplayFunctions")
      .setMaster("local")
    sc = new SparkContext(sparkConf)
    testRDDTuples = sc.parallelize(List(("jdoe","John DOE",32),("hsue","Helen SUE",27),("rsmith","Richard SMITH",45)))
    testRDDPersons = sc.parallelize(List(Person("jdoe","John DOE",32),Person("hsue","Helen SUE",27),Person("rsmith","Richard SMITH",45)))
  }

  override def beforeEach() {
    stream = new java.io.ByteArrayOutputStream()
    super.beforeEach() // To be stackable, must call super.beforeEach
  }


  "DisplayFunctions" should "generate correct column headers for tuples" in {

    Console.withOut(stream) {
      new DisplayFunctions[(String,String,Int)](testRDDTuples).displayAsTable("Login","Name","Age")
    }

    stream.toString("UTF-8") should be("%table Login\tName\tAge\n" +
      "jdoe\tJohn DOE\t32\n" +
      "hsue\tHelen SUE\t27\n" +
      "rsmith\tRichard SMITH\t45\n")
  }

  "DisplayFunctions" should "generate correct column headers for case class" in {

    Console.withOut(stream) {
      new DisplayFunctions[Person](testRDDPersons).displayAsTable("Login","Name","Age")
    }

    stream.toString("UTF-8") should be("%table Login\tName\tAge\n" +
      "jdoe\tJohn DOE\t32\n" +
      "hsue\tHelen SUE\t27\n" +
      "rsmith\tRichard SMITH\t45\n")
  }

  "DisplayFunctions" should "truncate exceeding column headers for tuples" in {

    Console.withOut(stream) {
      new DisplayFunctions[(String,String,Int)](testRDDTuples).displayAsTable("Login","Name","Age","xxx","yyy")
    }

    stream.toString("UTF-8") should be("%table Login\tName\tAge\n" +
      "jdoe\tJohn DOE\t32\n" +
      "hsue\tHelen SUE\t27\n" +
      "rsmith\tRichard SMITH\t45\n")
  }

  "DisplayFunctions" should "pad missing column headers with ColumnXXX for tuples" in {

    Console.withOut(stream) {
      new DisplayFunctions[(String,String,Int)](testRDDTuples).displayAsTable("Login")
    }

    stream.toString("UTF-8") should be("%table Login\tColumn2\tColumn3\n" +
      "jdoe\tJohn DOE\t32\n" +
      "hsue\tHelen SUE\t27\n" +
      "rsmith\tRichard SMITH\t45\n")
  }

  "DisplayUtils" should "display HTML" in {
    DisplayUtils.html() should be ("%html ")
    DisplayUtils.html("test") should be ("%html test")
  }

  "DisplayUtils" should "display img" in {
    DisplayUtils.img("http://www.google.com") should be ("<img src='http://www.google.com' />")
    DisplayUtils.img64() should be ("%img ")
    DisplayUtils.img64("abcde") should be ("%img abcde")
  }

  override def afterEach() {
    try super.afterEach() // To be stackable, must call super.afterEach
    stream = null
  }

  after {
    sc.stop()
  }


}
