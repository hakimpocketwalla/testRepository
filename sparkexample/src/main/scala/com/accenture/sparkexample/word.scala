package com.accenture.sparkexample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object word {
  def main(args: Array[String]): Unit = {
  println("Hello from main of object")
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val conf = new SparkConf().setAppName("My first Spark Job").setMaster("local[*]")
  val sc = new SparkContext(conf)
  
  val text = sc.textFile("food.txt")
  println("Number of lines in the file are: %d",text.count())
  
  
 }
}