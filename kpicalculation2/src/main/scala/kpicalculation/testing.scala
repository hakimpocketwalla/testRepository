package kpicalculation

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import java.util.Date
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.hadoop.fs._
import scala.collection.Seq
import java.net.URI
import com.databricks.spark.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.hadoop.fs._



object testing {

  def CurrentTimeStamp(): String =  
    {
      val timestamp: Long = System.currentTimeMillis / 1000
      return timestamp.toString()
    }

  def FileCopy(resultPath: String, scrPath: String, desPath: String, hdfs: FileSystem): Unit =
    {

      //val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val hadoopConf = new org.apache.hadoop.conf.Configuration()

      hdfs.delete(new Path(resultPath), true)
      var fs1 = hdfs.globStatus(new org.apache.hadoop.fs.Path(scrPath));
      var srcPath = fs1(0).getPath
      var dstPath = new Path(desPath)
      FileUtil.copy(srcPath.getFileSystem(hadoopConf), srcPath, dstPath.getFileSystem(hadoopConf), dstPath, false, hadoopConf);

    }

  def main(args: Array[String]): Unit = {
    //print("Testing")
    //val cis_file = "C:\\CEC_Documents\\Curated\\cis\\*.csv,C:\\CEC_Documents\\Curated\\cis\\*.csv".split(",").toSeq
    //val path = cis_file.split(",")
    //System.setProperty("hadoop.home.dir", "C:\\SparkDev")
    //System.setProperty("spark.sql.warehouse.dir", "C:\\spark-warehouse")

    //val warehouseLocation = "hdfs://california-dwr-hdfs/apps/hive/warehouse/"

    val spark = SparkSession
      .builder()
      .appName("Prop39_metadata")
      .config("spark.master", "yarn")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //.config("hive.metastore.uris", "thrift://hdp-pr-master-0004.ad.water.ca.gov:9083")
      //.config("spark.ui.port", "44040" )
      //.config("spark.driver.allowMultipleContexts", "true")
      //.enableHiveSupport()
      .getOrCreate()

    //val sparkconf = new SparkConf().setAppName("final_process").setMaster("local")
    //sparkconf.set("org.apache.spark.serializer.KryoSerializer", "spark_serializer")
    //val sc = new SparkContext(sparkconf)
    //val sqlContext = new SQLContext(sc)  
      
    
    //import spark.implicits._
    //import spark.sql
    //val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //val hadoopConf = new org.apache.hadoop.conf.Configuration()

    //val sample = 
      //spark.read.format("com.databricks.spark.xml").option("rowTag", "book").load("books.xml")
      //spark.read.format("com.databricks.spark.xml").option("rowTag", "book").option("mode", "PERMISSIVE").load("C:\\Users\\ankan.ghosh\\Downloads\\19647250000000_2014-2015_SCE_ELECTRIC_20161027\\19647250000000_2014-2015_SCE_ELECTRIC_20161027.xml")
      //val sample = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "ns2:utilDataProp39Report").load("C:\\Users\\ankan.ghosh\\Downloads\\19647250000000_2014-2015_SCE_ELECTRIC_20161027\\sample.xml")
      val sample = spark.read.format("com.databricks.spark.xml").option("rowTag", "ns2:utilDataProp39Report").load("/user/sonus/prop39/xmltest/*.XML")
            
     // sample.
      
      val sample2 = sample.select(sample.col("ns2:LEA_Customer_CDSCode"),
                                  sample.col("ns2:LEA_CustomerData.ns2:LEA_Data.ns2:LEA_StreetAddress"),
                                  sample.col("ns2:LEA_CustomerData.ns2:LEA_Data.ns2:LEA_City"),
                                  sample.col("ns2:LEA_CustomerData.ns2:LEA_Data.ns2:LEA_ZipPlus4"),
                                  sample.col("ns2:LEA_CustomerData.ns2:LEA_Data.ns2:LEA_CustomerName"),
                                  sample.col("ns2:SchoolSiteData.ns2:SchoolSite_CDSCode"),
                                  sample.col("ns2:SchoolSiteData.ns2:SchoolSite_ZipPlus4"),
                                  sample.col("ns2:SchoolSiteData.ns2:SchoolSite_StreetAddress"),
                                  sample.col("ns2:UsageData"),
                                  //functions.explode(sample.col("ns2:UsageData.ns2:ElectricityUsageData.ns2:ElectricityIntervalData")).as("IntervalData")
                                  functions.explode(sample.col("ns2:UsageData.ns2:ElectricityUsageData.ns2:ElectricityIntervalData.ns2:ElectricityIntervalDataForAgreement.ns2:IntervalBlockDay")).as("IntervalBlockDay")
                                  ) 
                                  
         //sample2.printSchema();                        
                                  
  /*    val sample3 = sample.select(sample2.col("ns2:LEA_Customer_CDSCode"),
                                  sample2.col("ns2:LEA_StreetAddress"),
                                  sample2.col("ns2:LEA_City"),
                                  sample2.col("ns2:LEA_ZipPlus4"),
                                  sample2.col("ns2:LEA_CustomerName"),
                                  sample2.col("ns2:SchoolSite_CDSCode"),
                                  sample2.col("ns2:SchoolSite_ZipPlus4"),
                                  sample2.col("ns2:SchoolSite_StreetAddress"),
                                  //sample2.col("IntervalData"),
                                  functions.explode(sample2.col("ns2:UsageData.ns2:ElectricityUsageData.ns2:ElectricityIntervalData.ns2:ElectricityIntervalDataForAgreement.ns2:IntervalBlockDay")).as("IntervalBlockDay")
                                  ) 
   
    */                                 
                                
        val sample4 = sample2.select(sample2.col("ns2:LEA_Customer_CDSCode"),
                                  sample2.col("ns2:LEA_StreetAddress"),
                                  sample2.col("ns2:LEA_City"),
                                  sample2.col("ns2:LEA_ZipPlus4"),
                                  sample2.col("ns2:LEA_CustomerName"),
                                  sample2.col("ns2:SchoolSite_CDSCode"),
                                  sample2.col("ns2:SchoolSite_ZipPlus4"),
                                  sample2.col("ns2:SchoolSite_StreetAddress"),
                                  //sample3.col("IntervalData"),
                                  //sample3.col("IntervalBlockDay"),
                                  functions.explode(sample2.col("IntervalBlockDay.ns2:IntervalReadings")).as("IntervalReadings")
                                  ) 
                                  
                                  
        val sample5 =  sample4.select(sample4.col("ns2:LEA_Customer_CDSCode").as("LEA_Customer_CDSCode"),
                                  sample4.col("ns2:LEA_StreetAddress").as("LEA_StreetAddress"),
                                  sample4.col("ns2:LEA_City").as("LEA_City"),
                                  sample4.col("ns2:LEA_ZipPlus4").as("LEA_ZipPlus4"),
                                  sample4.col("ns2:LEA_CustomerName").as("LEA_CustomerName"),
                                  sample4.col("ns2:SchoolSite_CDSCode").as("SchoolSite_CDSCode"),
                                  sample4.col("ns2:SchoolSite_ZipPlus4").as("SchoolSite_ZipPlus4"),
                                  sample4.col("ns2:SchoolSite_StreetAddress").as("SchoolSite_StreetAddress"),
                                  //sample4.col("IntervalData"),
                                  //sample4.col("IntervalBlockDay"),
                                  sample4.col("IntervalReadings.ns2:IntervalReadingEnergy.ns2:IntervalReading_EnergyUsage").as("IntervalReading_EnergyUsage"),
                                  sample4.col("IntervalReadings.ns2:IntervalReading_StartTime").as("IntervalReading_StartTime")
                                  )    
  
       val df: SimpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss") //Input format
       //val //formatter: SimpleDateFormat = new SimpleDateFormat("yyMM") //Output format
       //val yearmonth = udf((t2: String) => if (t2 != null) formatter.format(format.parse(t2)) else "")
                               
       val zip = udf((t1: String) => if (t1 == null || t1 == "") null else df.format(t1.toLong * 1000L) )                      
                                  
        val sample6 = sample5.withColumn("IntervalReading_StartTime", zip(col("IntervalReading_StartTime")))                        
          
        //sample6.show()
         //Writing the metadata in HDFS path
        val hadoopConf = new org.apache.hadoop.conf.Configuration()
        val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        
        //sample6.show()
        sample6.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/user/sonus/prop39/xmltest/temp")
       

  }
}