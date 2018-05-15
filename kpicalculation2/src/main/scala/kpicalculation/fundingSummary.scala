package kpicalculation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import java.sql.Timestamp
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.hadoop.fs._

object fundingSummary {
  
  def PresentDate(): String =
    {
      val d: Timestamp = new Timestamp(System.currentTimeMillis())
      val formattter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS")
      val TimeStamp = formattter.parse(d.toString()).toString()
      return TimeStamp
    }  
  
  def CurrentTimeStamp(): String =
    {
      val timestamp: Long = System.currentTimeMillis / 1000
      return timestamp.toString()
    }
   
    def main(args: Array[String]) = {

    val warehouseLocation = "hdfs://california-dwr-hdfs/apps/hive/warehouse/"

val spark = SparkSession
  .builder()
  .appName("CEC_fundingSummary")
  .config("spark.master", "yarn")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .config("hive.metastore.uris", "thrift://hdp-pr-master-0004.ad.water.ca.gov:9083")
  .config("spark.ui.port", "44040" )
  .config("spark.driver.allowMultipleContexts", "true")
  .enableHiveSupport()
  .getOrCreate()

  
    import spark.implicits._
    
  
  //Code Starts
    
    print(spark.sql("""show databases""").show()) //Testing Hive connectivity
      
    val fsa_tblSubmissionDF = spark.sql("""SELECT cast(cast((sum(GRANTAMOUNTREQUESTED)) as BIGINT)  as string) as amount_spent FROM prop39.curated_fsa_tblSubmissions WHERE StatusID=5""")
    val fsa_tblSubmissionDF_tmp = spark.sql("""SELECT SUBMISSIONID FROM prop39.curated_fsa_tblSubmissions WHERE StatusID=5""")
      
      //Union above with below
      
    val asa_tblSubmissionDF_tmp = spark.sql("""SELECT * FROM prop39.curated_asa_tblSubmissions WHERE StatusID=5""")
    
    //Taking out rows where submissiond id is not in fsa_tbSubmissions
    val asa_tblSubmissionDF = asa_tblSubmissionDF_tmp.join(fsa_tblSubmissionDF_tmp, Seq("SUBMISSIONID"), "leftanti") 
    val asa_tblAnnualReportingDF = spark.sql("""SELECT FISCALYEAR as FISCALYEAR_A , REPORTINGID FROM prop39.curated_asa_tblAnnualReporting""")
      
    val joinasaDF = asa_tblSubmissionDF.join(asa_tblAnnualReportingDF,Seq("REPORTINGID"))
    joinasaDF.createOrReplaceTempView("joinasaDF")
    
    //Getting max fiscal year for each original submission id
    val maxFiscalYearDF = spark.sql("""SELECT max(FISCALYEAR_A) as FISCALYEAR, ORISUBMISSIONID as ORISUBMISSIONID_A FROM joinasaDF group by ORISUBMISSIONID""")
  
    val join1DF = joinasaDF.join(maxFiscalYearDF, joinasaDF("FISCALYEAR_A") === maxFiscalYearDF("FISCALYEAR") && joinasaDF("ORISUBMISSIONID") === maxFiscalYearDF("ORISUBMISSIONID_A"), "inner")
    join1DF.createOrReplaceTempView("join1DF")
    val SelectionTypesDF = spark.sql("""SELECT SELECTIONDESCRIPTION,SELECTIONID FROM prop39.curated_upkeep_SelectionTypes""")
      
    val fiscalyearDF = join1DF.join(SelectionTypesDF,join1DF("""FISCALYEAR_A""") === SelectionTypesDF("""SELECTIONID"""))
    fiscalyearDF.createOrReplaceTempView("fiscalyearDF")
      
    val asa_sumDF = spark.sql("""SELECT sum(AmountRequestingforEnergyManager) + sum(AmountRequestingforTraining) + sum(Prop39ShareEstimatedTotal) as amount_spent FROM fiscalyearDF""")
    
    val union1DF = fsa_tblSubmissionDF.union(asa_sumDF)
    
    union1DF.createOrReplaceTempView("union1DF")
    
    //AmountSpent Dataframe
    val amountspentDF = spark.sql("""SELECT '1' as key, cast(cast((sum(amount_spent)) as BIGINT)  as string) as amount_spent FROM union1DF""")
    
    //AmountApproved Dataframe
    val tblSubmissionDF = spark.sql("""SELECT '1' as key, cast(cast((sum(grantamountrequested)) as BIGINT)  as string) as amount_approved FROM prop39.curated_tblsubmissions WHERE (StatusID=5 or StatusID=10) and (AAASubmissionID is NULL or AAASubmissionID='')""")
    
    val tblprop39_allocationsDF = spark.sql("""SELECT regexp_replace((substr(Total_Award_Allocation,4)),',','') as amount_allocated FROM prop39.curated_tblprop39_allocations""")
    tblprop39_allocationsDF.createOrReplaceTempView("tblprop39_allocationsDF")
    
    //AmountAllocated Dataframe
    val amount_allocatedDF = spark.sql("""SELECT '1' as key, cast(cast((sum(amount_allocated)) as BIGINT)  as string) as amount_allocated FROM tblprop39_allocationsDF""")
    
    val join1 = amountspentDF.join(tblSubmissionDF, Seq("key"))
    
    val join2 = join1.join(amount_allocatedDF, Seq("key"))
    
    join2.printSchema()
    join2.createOrReplaceTempView("join2")
    
    
    //Dropping previous Hive table if exists
    spark.sql("""drop table if exists prop39.purposebuilt_funding_summary""")
    
    //Pushing data to Hive internal table
    spark.sql("""create table prop39.purposebuilt_funding_summary as select amount_spent,amount_approved,amount_allocated from join2""")
    
    
    //Pushing data to Purposebuilt HDFS path
    join2.coalesce(1).write.format("""com.databricks.spark.csv""").option("delimiter", "\t").mode("overwrite").save("""/cec/purposebuilt/nonconf/prop39/program_funding_summary""")
  
    //Metadata Calculations begins here
    val schemaString = "Source,Destination,Size,Row_Count,Time"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    //List of all the files and counts
    val metadata_List = List(Row("Source TableName : prop39.curated_fsa_tblSubmissions", "/cec/purposebuilt/nonconf/prop39/program_funding_summary", "NIL", fsa_tblSubmissionDF.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_asa_tblSubmissions", "/cec/purposebuilt/nonconf/prop39/program_funding_summary", "NIL", asa_tblSubmissionDF_tmp.count().toString(), PresentDate()),
                             Row("Source TableName : prop39.curated_asa_tblAnnualReporting", "/cec/purposebuilt/nonconf/prop39/program_funding_summary", "NIL", asa_tblAnnualReportingDF.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_upkeep_SelectionTypes", "/cec/purposebuilt/nonconf/prop39/program_funding_summary", "NIL", SelectionTypesDF.count().toString(), PresentDate()),
                             Row("Source TableName : prop39.curated_tblsubmissions", "/cec/purposebuilt/nonconf/prop39/program_funding_summary", "NIL", tblSubmissionDF.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_tblprop39_allocations", "/cec/purposebuilt/nonconf/prop39/program_funding_summary", "NIL", tblprop39_allocationsDF.count().toString(), PresentDate()))
     
     val rdd = spark.sparkContext.makeRDD(metadata_List)
     val finalDF = spark.createDataFrame(rdd, schema)
     
     //Writing the metadata in HDFS path
     val hadoopConf = new org.apache.hadoop.conf.Configuration()
     val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
     //hdfs.delete(new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp"), true)
     //hdfs.mkdirs(new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp"))
     finalDF.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp")
     val fs3 = hdfs.globStatus(new org.apache.hadoop.fs.Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp/part-00000*.csv"));
     hdfs.rename(fs3(0).getPath, new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/meta_fundingSummary_"+CurrentTimeStamp()+".csv"))
     hdfs.delete(new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp/"), true)
    
     
    spark.stop()
}
}