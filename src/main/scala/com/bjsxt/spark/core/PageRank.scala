package com.bjsxt.spark.core

import java.io.File

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * 第一元素为当前页面，后面的为出链的页面
A   B   D
B   C
C   B   D
D   B   C
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    val dataFile = "file:///Users/zhouxiaowu/Downloads/install-package/Spark-Example-master/data/page_rank_data.txt"
    val resultFile = "file:///Users/zhouxiaowu/Downloads/install-package/Spark-Example-master/data/page_rank_result"
    val spark = SparkSession.builder().appName("Simple App").master("local").getOrCreate()
    val sc = spark.sparkContext

    val rdd = sc.textFile(dataFile)



    /**
      * 计算页面总数
      * distinct = map.reduceByKey.map
      */
    val pageCount = rdd.flatMap(t => {t.split("\t")}).distinct().count()

    //广播变量
    val brpc = sc.broadcast(pageCount)

    /**
      * 累加器：集群规模间的一个大变量
      * 累加器是维护在Driver中
      * 每一个Executor可以操作累积器中的中，这里面线程安全，以及变量共享问题 Driver端已经解决
      * 注意点：
      * 		1、在Driver定义
      * 		2、累积器中值只能在Driver端读取
      */
    //创建一个double类型的累加器   这个累加器只能累加double类型的值
    val acc = sc.doubleAccumulator("error")


    var nodeRdd = rdd.map(t => {
      val items = t.split("\t")
      val key = items(0)
      val node = Node(1.0, items.drop(1))
      (key,node)
    })

    val limit = 0.01
    var flag = true

    while (flag){
      /**
        * 使用flatMap来对相邻的节点进行投票
        * 		1、计算票面值
        * 		2、投票
        */
      val pageRankRdd = nodeRdd.flatMap(t => {
        val page = t._1
        val node = t._2
        val pageRank = node.pr
        val adjacentSize = node.adjacentNodes.length
        val pmz = pageRank / adjacentSize
        node.adjacentNodes.map(d => (d,pmz) )
      }).reduceByKey(_+_)

      //(key,(W,V))
      nodeRdd = nodeRdd.join(pageRankRdd).map(t => {
        val page = t._1
        val node = t._2._1
        val newPr = (1-0.85)/brpc.value + 0.85*t._2._2

        acc.add(Math.abs(newPr - node.pr))

//        println(acc.value)
        node.pr = newPr
        (page,node)
      })

      nodeRdd.count()
//      println("error: " + acc.value)
      if (acc.value < limit){
        flag = false
      }

      acc.reset()
    }

    nodeRdd.foreach(x=>{
      val page = x._1
      val pr = x._2.pr
      println("page:" + page + "=" + pr)
    })

    spark.close()
  }

}

case class Node(var pr:Double, var adjacentNodes:Array[String])
