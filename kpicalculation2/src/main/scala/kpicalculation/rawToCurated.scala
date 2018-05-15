package kpicalculation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.hadoop.fs._

object rawToCurated {
  
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

  def FileCopy(resultPath: String, scrPath: String, desPath: String, hdfs : FileSystem): Unit =
    {

      
      val hadoopConf = new org.apache.hadoop.conf.Configuration()

      hdfs.delete(new Path(resultPath), true)
      var fs1 = hdfs.globStatus(new org.apache.hadoop.fs.Path(scrPath));
      var srcPath = fs1(0).getPath
      var dstPath = new Path(desPath)
      FileUtil.copy(srcPath.getFileSystem(hadoopConf), srcPath, dstPath.getFileSystem(hadoopConf), dstPath, false, hadoopConf);

    }
  
  def main(args: Array[String])  {

    val warehouseLocation = "hdfs://california-dwr-hdfs/apps/hive/warehouse/"

val spark = SparkSession
  .builder()
  .appName("CEC_rawToCurated")
  .config("spark.master", "yarn")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .config("hive.metastore.uris", "thrift://hdp-pr-master-0004.ad.water.ca.gov:9083")
  .config("spark.ui.port", "44040" )
  .config("spark.driver.allowMultipleContexts", "true")
  .enableHiveSupport()
  .getOrCreate()

  
    import spark.implicits._
    
  
  //Code Starts

val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
val hadoopConf = new org.apache.hadoop.conf.Configuration()
    
     
    FileCopy("/cec/ed/curated/nonconf/peps/siteinformation/asa/", "/cec/ed/raw/nonconf/peps/siteinformation/asa/" , "/cec/ed/curated/nonconf/peps/siteinformation/", hdfs)
    
//spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/siteinformation/asa/asa_tblSiteInformation.csv' OVERWRITE INTO TABLE prop39.curated_asa_tblSiteInformation")

     
    FileCopy("/cec/ed/curated/nonconf/peps/siteinformation/general/", "/cec/ed/raw/nonconf/peps/siteinformation/general/" , "/cec/ed/curated/nonconf/peps/siteinformation/", hdfs)
    
//spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/siteinformation/general/tblSiteInformation.csv' OVERWRITE INTO TABLE prop39.curated_tblSiteInformation")

     
   FileCopy("/cec/ed/curated/nonconf/peps/siteinformation/fsa/", "/cec/ed/raw/nonconf/peps/siteinformation/fsa/" , "/cec/ed/curated/nonconf/peps/siteinformation/", hdfs)
    
 // spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/siteinformation/fsa/fsa_tblSiteInformation.csv' OVERWRITE INTO TABLE prop39.curated_fsa_tblSiteInformation")

   
   FileCopy("/cec/ed/curated/nonconf/peps/submissions/general/", "/cec/ed/raw/nonconf/peps/submissions/general/" , "/cec/ed/curated/nonconf/peps/submissions/", hdfs)
   
 //  spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/submissions/general/tblSubmissions.csv' OVERWRITE INTO TABLE prop39.curated_tblSubmissions")

  
   FileCopy("/cec/ed/curated/nonconf/peps/submissions/asa/", "/cec/ed/raw/nonconf/peps/submissions/asa/" , "/cec/ed/curated/nonconf/peps/submissions/", hdfs)
   
 //  spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/submissions/asa/asa_tblSubmissions.csv' OVERWRITE INTO TABLE prop39.curated_asa_tblSubmissions")

  
   FileCopy("/cec/ed/curated/nonconf/peps/submissions/fsa/", "/cec/ed/raw/nonconf/peps/submissions/fsa/" , "/cec/ed/curated/nonconf/peps/submissions/", hdfs)
   
//   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/submissions/fsa/fsa_tblSubmissions.csv' OVERWRITE INTO TABLE prop39.curated_fsa_tblSubmissions")

   
   FileCopy("/cec/ed/curated/nonconf/peps/reporting/asa/", "/cec/ed/raw/nonconf/peps/reporting/asa/" , "/cec/ed/curated/nonconf/peps/reporting/", hdfs)
    
//   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/annualreporting/asa/asa_tblAnnualReporting.csv' OVERWRITE INTO TABLE prop39.curated_asa_tblAnnualReporting")

    FileCopy("/cec/ed/curated/nonconf/peps/reporting/fsa/", "/cec/ed/raw/nonconf/peps/reporting/fsa/" , "/cec/ed/curated/nonconf/peps/reporting/", hdfs)
    
//   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/finalreporting/fsa/fsa_tblFinalReporting.csv' OVERWRITE INTO TABLE prop39.curated_fsa_tblFinalReporting")

     FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/statustypes/", "/cec/ed/global/raw/nonconf/peps/referencedata/statustypes/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
   
//   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/statustypes/tblUtilityStatusTypes.csv' OVERWRITE INTO TABLE prop39.curated_tblUtilityStatusType")

     
    FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/submissionstatus/", "/cec/ed/global/raw/nonconf/peps/referencedata/submissionstatus/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
   
//   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/submissionstatus/tblSubmissionsStatus.csv' OVERWRITE INTO TABLE prop39.curated_tblSubmissionsStatus")

     
    FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/publicschools/", "/cec/ed/global/raw/nonconf/peps/referencedata/publicschools/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
   
    
//    spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/publicschools/Public_Schools.csv' OVERWRITE INTO TABLE prop39.curated_upkeep_PublicSchools")

     
    FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/districttypes/", "/cec/ed/global/raw/nonconf/peps/referencedata/districttypes/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
    
//    spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/districttypes/DistrictTypes.csv' OVERWRITE INTO TABLE prop39.curated_upkeep_DistrictTypes")

      
    FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/selectiontypes/", "/cec/ed/global/raw/nonconf/peps/referencedata/selectiontypes/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
    
//    spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/selectiontypes/SelectionTypes.csv' OVERWRITE INTO TABLE prop39.curated_upkeep_SelectionTypes")

    
    FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/leacode/", "/cec/ed/global/raw/nonconf/peps/referencedata/leacode/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
    
//    spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/leacode/leacodes.csv' OVERWRITE INTO TABLE prop39.curated_tblleacodes")

 
    FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/IMD_XML_Conversion/", "/cec/ed/global/raw/nonconf/peps/referencedata/IMD_XML_Conversion/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
    
 //   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/IMD_XML_Conversion/XML_Conversion.csv' OVERWRITE INTO TABLE prop39.curated_tblIMD_XML_Conversion") 
 
      
    FileCopy("/cec/ed/curated/nonconf/peps/efficiencymeasure/general/", "/cec/ed/raw/nonconf/peps/efficiencymeasure/general/" , "/cec/ed/curated/nonconf/peps/efficiencymeasure/", hdfs)
    
//   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/efficiencymeasure/general/tblEfficiencyMeasure.csv' OVERWRITE INTO TABLE prop39.curated_tblEfficiencyMeasure") 

      
    FileCopy("/cec/ed/curated/nonconf/peps/photovoltaicmeasure/general/", "/cec/ed/raw/nonconf/peps/photovoltaicmeasure/general/" , "/cec/ed/curated/nonconf/peps/photovoltaicmeasure/", hdfs)
    
//   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/photovoltaicmeasure/general/tblPhotovoltaicMeasure.csv' OVERWRITE INTO TABLE prop39.curated_tblPhotovoltaicMeasure") 

     
    FileCopy("/cec/ed/global/curated/nonconf/peps/referencedata/elsi/", "/cec/ed/global/raw/nonconf/peps/referencedata/elsi/" , "/cec/ed/global/curated/nonconf/peps/referencedata/", hdfs)
 
//  spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/global/raw/nonconf/peps/referencedata/esli/else_report.csv' OVERWRITE INTO TABLE prop39.curated_tblelse_report") 

     
    FileCopy("/cec/ed/curated/nonconf/peps/photovoltaicmeasure/asa/", "/cec/ed/raw/nonconf/peps/photovoltaicmeasure/asa/" , "/cec/ed/curated/nonconf/peps/photovoltaicmeasure/", hdfs)

//spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/photovoltaicmeasure/asa/asa_tblPhotovoltaicMeasure.csv' OVERWRITE INTO TABLE prop39.curated_asa_tblPhotovoltaicMeasure") 

    
    FileCopy("/cec/ed/curated/nonconf/peps/technologysummary/", "/cec/ed/raw/nonconf/peps/technologysummary/" , "/cec/ed/curated/nonconf/peps/", hdfs)
     
//    spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/technologysummary/TechnologySummaryTable.csv' OVERWRITE INTO TABLE prop39.curated_TechnologySummaryTable")

    
    FileCopy("/cec/ed/curated/nonconf/peps/districts/general/", "/cec/ed/raw/nonconf/peps/districts/general/" , "/cec/ed/curated/nonconf/peps/districts/", hdfs)
  
 //    spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/districts/general/tblDistricts.csv' OVERWRITE INTO TABLE prop39.curated_tblDistricts")

      
    FileCopy("/cec/ed/curated/nonconf/peps/sitesummary/general/", "/cec/ed/raw/nonconf/peps/sitesummary/general/" , "/cec/ed/curated/nonconf/peps/sitesummary/", hdfs)
  
 //   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/sitesummary/general/tblSiteSummary.csv' OVERWRITE INTO TABLE prop39.curated_tblSiteSummary")

     
    FileCopy("/cec/ed/curated/nonconf/peps/sitesummary/fsa/", "/cec/ed/raw/nonconf/peps/sitesummary/fsa/" , "/cec/ed/curated/nonconf/peps/sitesummary/", hdfs)
  
//    spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/sitesummary/fsa/fsa_tblSiteSummary.csv' OVERWRITE INTO TABLE prop39.curated_fsa_tblSiteSummary")
  
    
    FileCopy("/cec/ed/curated/nonconf/peps/allocations/", "/cec/ed/raw/nonconf/peps/allocations/" , "/cec/ed/curated/nonconf/peps/", hdfs)
    
 //   spark.sql("LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/allocations/prop39_allocations.csv OVERWRITE INTO TABLE prop39.curated_tblprop39_allocations")
    


//MetaData Calculation started
    val schemaString = "Source,Destination,Size,Row_Count,Time"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val siteinformation_asa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/siteinformation/asa/*.csv")
    val siteinformation_general = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/siteinformation/general/*.csv")
    val siteinformation_fsa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/siteinformation/fsa/*.csv")
    val submissions_general = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/submissions/general/*.csv")
    val submissions_asa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/submissions/asa/*.csv")
    val submissions_fsa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/submissions/fsa/*.csv")
    val annualreporting_asa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/reporting/asa/*.csv")
    val finalreporting_fsa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/reporting/fsa/*.csv")
    val referencedata_statustypes = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/statustypes/*.csv")
    val referencedata_submissionstatus  = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/submissionstatus/*.csv") 
    val referencedata_publicschools = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/publicschools/*.csv")
    val referencedata_districttypes = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/districttypes/*.csv")
    val referencedata_selectiontypes = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/selectiontypes/*.csv")
    val referencedata_leacode = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/leacode/*.csv")
    val referencedata_IMD_XML_Conversion = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/IMD_XML_Conversion/*")
    val efficiencymeasure_general = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/efficiencymeasure/general/*.csv")
    val photovoltaicmeasure_general = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/photovoltaicmeasure/general/*.csv")
    val referencedata_esli = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/global/raw/nonconf/peps/referencedata/elsi/*.csv")
    val photovoltaicmeasure_asa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/photovoltaicmeasure/asa/*.csv")
    val technologysummary = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/technologysummary/*.csv")
    val districts_general = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/districts/general/*.csv")
    val sitesummary_general = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/sitesummary/general/*.csv")
    val sitesummary_fsa = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/sitesummary/fsa/*.csv")
    val allocations = spark.read.format("com.databricks.spark.csv").option("header", "true").option("mode", "permissive").option("delimiter", "\t").load("/cec/ed/raw/nonconf/peps/allocations/*.csv")
   
    
    val metadata_List = List(Row("siteinformation_asa paths : /cec/ed/raw/nonconf/peps/siteinformation/asa/*.csv", "/cec/ed/curated/nonconf/peps/siteinformation/asa/", "NIL", siteinformation_asa.count().toString(), PresentDate()), 
                    Row("siteinformation_general paths : /cec/ed/raw/nonconf/peps/siteinformation/general/*.csv", "/cec/ed/curated/nonconf/peps/siteinformation/general/", "NIL", siteinformation_general.count().toString(), PresentDate()),
                    Row("siteinformation_fsa path : /cec/ed/raw/nonconf/peps/siteinformation/fsa/*.csv", "/cec/ed/curated/nonconf/peps/siteinformation/fsa/", "NIL", siteinformation_fsa.count().toString(), PresentDate()), 
                    Row("submissions_general paths : /cec/ed/raw/nonconf/peps/submissions/general/*.csv", "/cec/ed/curated/nonconf/peps/submissions/general/", "NIL", submissions_general.count().toString(), PresentDate()),
                    Row("submissions_asa paths : /cec/ed/raw/nonconf/peps/submissions/asa/*.csv", "/cec/ed/curated/nonconf/peps/submissions/asa/", "NIL", submissions_asa.count().toString(), PresentDate()),
                    Row("submissions_fsa paths : /cec/ed/raw/nonconf/peps/submissions/fsa/*.csv", "/cec/ed/curated/nonconf/peps/submissions/fsa/", "NIL", submissions_fsa.count().toString(), PresentDate()),
                    Row("annualreporting_asa paths : /cec/ed/raw/nonconf/peps/reporting/asa/*.csv", "/cec/ed/curated/nonconf/peps/reporting/asa", "NIL", annualreporting_asa.count().toString(), PresentDate()),
                    Row("finalreporting_fsa paths : /cec/ed/raw/nonconf/peps/reporting/fsa*.csv", "/cec/ed/curated/nonconf/peps/reporting/fsa", "NIL", finalreporting_fsa.count().toString(), PresentDate()),
                    Row("referencedata_statustypes paths : /cec/ed/global/raw/nonconf/peps/referencedata/statustypes/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/statustypes/", "NIL", referencedata_statustypes.count().toString(), PresentDate()),
                    Row("referencedata_submissionstatus paths : /cec/ed/global/raw/nonconf/peps/referencedata/submissionstatus/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/submissionstatus/", "NIL", referencedata_submissionstatus.count().toString(), PresentDate()),
                    Row("referencedata_publicschools paths : /cec/ed/global/raw/nonconf/peps/referencedata/publicschools/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/publicschools/", "NIL", referencedata_publicschools.count().toString(), PresentDate()),
                    Row("referencedata_districttypes paths : /cec/ed/global/raw/nonconf/peps/referencedata/districttypes/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/districttypes/", "NIL", referencedata_districttypes.count().toString(), PresentDate()),
                    Row("referencedata_selectiontypes paths : /cec/ed/global/raw/nonconf/peps/referencedata/selectiontypes/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/selectiontypes/", "NIL", referencedata_selectiontypes.count().toString(), PresentDate()),
                    Row("referencedata_leacode paths : /cec/ed/global/raw/nonconf/peps/referencedata/leacode/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/leacode/", "NIL", referencedata_leacode.count().toString(), PresentDate()),
                    Row("referencedata_IMD_XML_Conversion paths : /cec/ed/global/raw/nonconf/peps/referencedata/IMD_XML_Conversion/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/IMD_XML_Conversion/", "NIL", referencedata_IMD_XML_Conversion.count().toString(), PresentDate()),
                    Row("efficiencymeasure_general paths : /cec/ed/raw/nonconf/peps/efficiencymeasure/general/*.csv", "/cec/ed/curated/nonconf/peps/efficiencymeasure/general/", "NIL", efficiencymeasure_general.count().toString(), PresentDate()),
                    Row("photovoltaicmeasure_general paths : /cec/ed/raw/nonconf/peps/photovoltaicmeasure/general/*.csv", "/cec/ed/curated/nonconf/peps/photovoltaicmeasure/general/", "NIL", photovoltaicmeasure_general.count().toString(), PresentDate()),
                    Row("referencedata_esli paths : /cec/ed/global/raw/nonconf/peps/referencedata/elsi/*.csv", "/cec/ed/global/curated/nonconf/peps/referencedata/esli/", "NIL", referencedata_esli.count().toString(), PresentDate()),
                    Row("photovoltaicmeasure_asa paths : /cec/ed/raw/nonconf/peps/photovoltaicmeasure/asa/*.csv", "/cec/ed/curated/nonconf/peps/photovoltaicmeasure/asa/", "NIL", photovoltaicmeasure_asa.count().toString(), PresentDate()),
                    Row("technologysummary paths : /cec/ed/raw/nonconf/peps/technologysummary/*.csv", "/cec/ed/curated/nonconf/peps/technologysummary/", "NIL", technologysummary.count().toString(), PresentDate()),
                    Row("districts_general paths : /cec/ed/raw/nonconf/peps/districts/general/*.csv", "/cec/ed/curated/nonconf/peps/districts/general/", "NIL", districts_general.count().toString(), PresentDate()),
                    Row("sitesummary_general paths : /cec/ed/raw/nonconf/peps/sitesummary/general/*.csv", "/cec/ed/curated/nonconf/peps/sitesummary/general/", "NIL", sitesummary_general.count().toString(), PresentDate()),
                    Row("sitesummary_fsa paths : /cec/ed/raw/nonconf/peps/sitesummary/fsa/*.csv", "/cec/ed/curated/nonconf/peps/sitesummary/fsa/", "NIL", sitesummary_fsa.count().toString(), PresentDate()),
                    Row("allocations path : /cec/ed/raw/nonconf/peps/allocations/*.csv", "/cec/ed/curated/nonconf/peps/allocations/", "NIL", allocations.count().toString(), PresentDate()))
     
                  
     val rdd = spark.sparkContext.makeRDD(metadata_List)
     val finalDF = spark.createDataFrame(rdd, schema)
     
  //   val hadoopConf = new org.apache.hadoop.conf.Configuration()
  //   val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
     hdfs.delete(new Path("/cec/ed/raw/nonconf/peps/metadata"), true)
     //hdfs.cp(new Path("/cec/ed/raw/nonconf/peps/metadata"), new Path("/cec/ed/raw/nonconf/peps/metadata"))
     //hdfs.mkdirs(new Path("/cec/ed/raw/nonconf/peps/metadata"))
     finalDF.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/cec/ed/raw/nonconf/peps/metadata/")
     val fs3 = hdfs.globStatus(new org.apache.hadoop.fs.Path("/cec/ed/raw/nonconf/peps/metadata/part-00000*.csv"));
     hdfs.rename(fs3(0).getPath, new Path("/cec/ed/raw/nonconf/peps/metadata/Metadata_"+CurrentTimeStamp()+".csv"))
     //hdfs.delete(new Path("/cec/ed/raw/nonconf/peps/metadata/temp/"), true)


  }
}