package citi.rio.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Utils {
  def   getSession(name: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(name)
      .master("local[*]")
      //      .config("spark.sql.warehouse.dir", "file:///C:/temp") // only on windows
      .getOrCreate()
    spark
  }

  def getContext(name: String): SparkContext = {
    val conf = new SparkConf().setAppName(name)
    //setMaster("local") 本机的spark就用local，远端的就写ip
    //如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc
  }
}
