package accenture.DemoSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Test {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val text = sc.textFile("food.txt")
    val finalRdd = text.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _)
    finalRdd.foreach(println)
    
  }
  
}