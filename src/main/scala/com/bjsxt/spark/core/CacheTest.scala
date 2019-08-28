package com.bjsxt.spark.core

import org.apache.spark.sql.SparkSession

/**
 * 问题：文件太大的话，这个方法就不行了吧
 * 		内存存不下 导致OOM ×
 * 		如果文件太大，内存根本无法完全存储，还使用cache做持久化，此时并不会OOM,内存能存多少就存多少，存不下的丢掉
 * 		cache进行持久化的时候，持久化的单位是partiton
 */
object CacheTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CacheTest").master("local").getOrCreate()
    
    
    val sc = spark.sparkContext
    var rdd1 = sc.textFile("file:///d:/original")
    rdd1 = rdd1.cache
    
    val startTime = System.currentTimeMillis()
    val count = rdd1.count()
    val endTime = System.currentTimeMillis()
    println("count:" + count + "\tdurations:" + (endTime-startTime) + " ms")
    
    val startTime1 = System.currentTimeMillis()
    val count1 = rdd1.count()
    val endTime1 = System.currentTimeMillis()
    println("count:" + count1 + "\tdurations:" + (endTime1-startTime1) + " ms")
    
    while(true){}
//    spark.close()
  }
}