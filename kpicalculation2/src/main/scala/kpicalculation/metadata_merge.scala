package kpicalculation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.hadoop.fs._

object metadata_merge {
  
   def main(args: Array[String]) = {

    val warehouseLocation = "hdfs://california-dwr-hdfs/apps/hive/warehouse/"

  val spark = SparkSession
  .builder()
  .appName("Prop39_metadata")
  .config("spark.master", "yarn")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .config("hive.metastore.uris", "thrift://hdp-pr-master-0004.ad.water.ca.gov:9083")
  .config("spark.ui.port", "44040" )
  .config("spark.driver.allowMultipleContexts", "true")
  .enableHiveSupport()
  .getOrCreate()

  
    import spark.implicits._
    
    //Code starts here
    val meta_filepath = "/cec/purposebuilt/nonconf/prop39/metadata/temp/*.csv"
    val meta_files = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").load(meta_filepath)
    
    
     val hadoopConf = new org.apache.hadoop.conf.Configuration()
     val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
     hdfs.delete(new Path("/cec/purposebuilt/nonconf/prop39/metadata/metaData_Curated.csv"))
     //hdfs.mkdirs(new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp"))
     meta_files.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/cec/purposebuilt/nonconf/prop39/temp/")
     val fs3 = hdfs.globStatus(new org.apache.hadoop.fs.Path("/cec/purposebuilt/nonconf/prop39/temp/part-00000*.csv"));
     hdfs.rename(fs3(0).getPath, new Path("/cec/purposebuilt/nonconf/prop39/metadata/metaData_Curated.csv"))
     hdfs.delete(new Path("/cec/purposebuilt/nonconf/prop39/temp/"), true)
    
    
   }
}