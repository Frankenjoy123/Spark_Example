package com.sbw.spark.StructedStreaming

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType

object StructedStreamingInteKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sparkSession = SparkSession
                          .builder()
                          .appName("StructedStreaming")
                          .master("local")
                          .config("spark.sql.shuffle.partitions", "10")
                          .getOrCreate()

    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val linesDF = sparkSession
                .readStream
//                .format("Rate")
//                .option("rowsPerSecond", "100")
//                .option("rampUpTime ", 1)
                .format("socket")
                .option("host", "node01")
                .option("port", 9999)
                .schema(userSchema)
                .load()
    import sparkSession.implicits._
//    val wordDS = linesDF.as[String].flatMap(_.split(" "))
//    val wordCount = wordDS.groupBy("name").count()
    linesDF.printSchema
    val query = linesDF
                    .writeStream
                    .outputMode("Append")
                    .format("console")
                    .start()
    query.awaitTermination()
                    
  }
}