package com.bjsxt.spark.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.rdd.RDD

/**
 * Spark1.6  1.x
 * Spark2.3.1 2.x
 * 2.x 与 1.x   API高层封装了  增加了新功能  2.x StructedStream
 */
object WC {
  def main(args: Array[String]): Unit = {
    /**
     * 创建一个Spark会话
     * appName设置App的名字
     * master：设置App的运行模式(local standalone yarn mesos)
     */
    
    val spark = SparkSession
                      .builder()
                      .appName("WordCount")
                      .master("local")
                      .getOrCreate()
     
    val sc = spark.sparkContext
    
    val rdd1:RDD[String] = sc.textFile("file:///Users/zhouxiaowu/big-data/hello.txt")
    
    val wordRDD:RDD[String] = rdd1.flatMap { x => {
      println("flatMap:" + x)
      x.split(" ") 
      }
    }
    
    val pairRDD = wordRDD.map { x => {
      println("map" + x)
      (x,1)
    } }
    
    val resultRDD = pairRDD.reduceByKey((v1,v2) => v1+v2)
    
    resultRDD.saveAsTextFile("file:///Users/zhouxiaowu/big-data/hello-result")
    
    spark.close()
  }
}