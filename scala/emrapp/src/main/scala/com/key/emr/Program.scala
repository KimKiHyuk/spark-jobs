package com.key.emr
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object Program {

  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("avro etl")
      .getOrCreate()

    import spark.implicits._
    val input = spark.read.textFile("input.txt")
    
    val words = input.flatMap(line => line.split(" "))
    val WordGroup = words.groupByKey(_.toLowerCase())

    println("Word Count—->"+WordGroup.count().show())
  }
}