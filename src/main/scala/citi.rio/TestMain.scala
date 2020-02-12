package citi.rio

import citi.rio.domain.Student
import citi.rio.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object TestMain {
  // -Dhadoop.home.dir=C:\Users\kenneth\Documents\spark\hadoop-2.6\
  System.setProperty("hadoop.home.dir", "C:\\Users\\kenneth\\Documents\\spark\\hadoop-2.6\\")

  def main(args: Array[String]): Unit = {
    println("test scala")
    showAll()
    dataFrameTest()
  }

  def showAll(): Unit = {
    val sc = Utils.getContext(name = "Yume Core")
    val input = sc.textFile("./src/main/resources/students.csv")
    println("partitions: " + input.getNumPartitions)
    println("all students data, count: " + input.count())

    // first row is title, dont need
    // val studentsRdd = input.zipWithIndex.filter(_._2 > 2).keys // error count
    val studentsRdd = input.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

    println("all students data, count: " + studentsRdd.count())
    studentsRdd.foreach(println)
  }

  def dataFrameTest(): Unit = {
    val spark = Utils.getSession(name = "Yume Core SQL")
    val inputCSV = spark.read.csv("./src/main/resources/students.csv")

    val stuDF: RDD[Row] = inputCSV.rdd.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }
    //    stuDF.foreach(stu => println(stu.getString(0) + ": " + stu.getString(2).toInt))

    import spark.implicits._
    val ds = stuDF.map(parseEach).toDS()
    ds.foreach(stu => println(stu.name + ": " + stu.age))

  }

  def parseEach(row: Row): Student = {
    var student: Student = null
    try {
      student = Student(row.getString(0), row.getString(1), row.getString(2).toInt,
        row.getString(3).toInt, row.getString(4))
    } catch {
      case e: ClassCastException => student = Student(row.getString(0), row.getString(1), 0, 0, "0")
    }
    student
  }
}
