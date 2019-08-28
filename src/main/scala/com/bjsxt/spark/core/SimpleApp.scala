package com.bjsxt.spark.core

import org.apache.spark.sql.SparkSession

/**
  * Created by xiaowu.zhou on 2019/7/30. 
  */
object SimpleApp {

  def main(args: Array[String]): Unit = {
    val logFile = "file:///Users/zhouxiaowu/big-data/hello.txt"
    val spark = SparkSession.builder()
      .appName("Simple App")
      .master("local")
      .getOrCreate()
    val logData = spark.read.textFile(logFile).cache()

    val numAs = logData.filter(line => line.contains("xiaoming")).count()
    val numBs = logData.filter(line => line.contains("hello")).count()

    println(s"Line with xiaoming: $numAs, Lines with hello : $numBs")
    spark.stop()

  }

}
