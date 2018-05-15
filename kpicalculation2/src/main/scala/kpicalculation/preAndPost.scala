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

object preAndPost {
  
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
  .appName("CEC_Pre&Post")
  .config("spark.master", "yarn")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .config("hive.metastore.uris", "thrift://hdp-pr-master-0004.ad.water.ca.gov:9083")
  .config("spark.ui.port", "44040" )
  .config("spark.driver.allowMultipleContexts", "true")
  .enableHiveSupport()
  .getOrCreate()
  
	import spark.implicits._
    	  
    
  //Code Starts
      
      //XML IMD conversion
      spark.sql("""DROP TABLE prop39.xmlimdtemp""")
      
      spark.sql("""
      CREATE TABLE prop39.xmlimdtemp AS
		SELECT 
		A.cdscode,
		A.streetaddress,
		concat(substring(A.intervalreading_starttime, 7, 4), '-', substring(A.intervalreading_starttime, 1, 2), '-', substring(A.intervalreading_starttime, 4, 2)) starttimedaily,
		concat(substring(A.intervalreading_starttime, 7, 4), '-', substring(A.intervalreading_starttime, 1, 2)) starttimemonthly,
		A.intervalreadingenergydelivered
		FROM 
		(
		SELECT DISTINCT CDSCODE, streetaddress, intervalreading_starttime, intervalreadingenergydelivered
		FROM  
		prop39.curated_tblimd_xml_conversion
		) A
      """)
      
      //Pre Installation Dataframe
    
    val pre_installationDF=spark.sql("""
    SELECT
      A.SchoolSiteCDSCode cdscode,
      A.kWhPurchaseFromUtility pre_peps,
      A.SUBMISSIONID,
      Sum(B.intervalreading) pre_imd
      FROM
      (
		  SELECT
		  A.SchoolSiteCDSCode, 
		  A.EnergyBillFiscalYear, 
		  A.kWhPurchaseFromUtility,
		  A.submissionid,
		  concat(SUBSTR(C.selectiondescription, 1,4),'-','07') fiscalyear_A, 
		  concat('20',substring(C.selectiondescription, 6, 2),'-','06') fiscalyear_B 
		  FROM
		  (
			  SELECT 
			  cast (A.SchoolSiteCDSCode as bigint) SchoolSiteCDSCode, 
			  A.EnergyBillFiscalYear, 
			  CASE WHEN 
			  A.kWhPurchaseFromUtility IS NULL THEN A.totalannualelectricusekwh
			  ELSE A.kWhPurchaseFromUtility END kWhPurchaseFromUtility,
			  c.submissionid
			  FROM
			  prop39.curated_tblSiteInformation A
			  JOIN 
			  prop39.curated_tblsitesummary B 
			  ON (A.SSID=B.SSID)
			  JOIN 
			  prop39.curated_tblsubmissions C
			  ON(B.SUBMISSIONID= C.SUBMISSIONID)
			  WHERE (C.StatusID=5 or C.StatusID=10) and C.AAASubmissionID is NULL
		  ) A
		  LEFT OUTER JOIN
		  (
			  SELECT 
			  DISTINCT 
			  SELECTIONID, 
			  selectiondescription 
			  FROM prop39.curated_upkeep_selectiontypes 
		  ) C
		  ON(A.EnergyBillFiscalYear=C.SELECTIONID)
      ) A
      LEFT JOIN
      (
		  SELECT 
		  A.cdscode,
		  A.streetaddress,
		  A.starttimemonthly, 
		  SUM(A.intervalreadingenergydelivered) intervalreading
		  FROM prop39.xmlimdtemp A
		  GROUP BY
		  A.cdscode,
		  A.streetaddress,
		  A.starttimemonthly
      ) B
      ON(A.SchoolSiteCDSCode=B.cdscode)
      WHERE 
      B.starttimemonthly>=A.fiscalyear_A AND B.starttimemonthly<=A.fiscalyear_B
      GROUP BY
      A.SchoolSiteCDSCode,
      A.kWhPurchaseFromUtility,
      A.SUBMISSIONID
      """)
        
        
      pre_installationDF.createOrReplaceTempView("pre_installationDF")
    	
    	
    	val pre_test=spark.sql("""SELECT 
      A.SchoolSiteCDSCode cdscode,
      A.kWhPurchaseFromUtility pre_peps,
      A.submissionid,
      B.pre_imd
      FROM
      (
      SELECT 
      cast (A.SchoolSiteCDSCode as bigint) SchoolSiteCDSCode, 
      A.EnergyBillFiscalYear, 
      CASE WHEN 
      A.kWhPurchaseFromUtility IS NULL THEN A.totalannualelectricusekwh
      ELSE A.kWhPurchaseFromUtility END kWhPurchaseFromUtility,
      c.submissionid 
      FROM
      prop39.curated_tblSiteInformation A
      JOIN 
      prop39.curated_tblsitesummary B 
      ON (A.SSID=B.SSID)
      JOIN 
      prop39.curated_tblsubmissions C
      ON(B.SUBMISSIONID= C.SUBMISSIONID)
      WHERE (C.StatusID=5 or C.StatusID=10) and C.AAASubmissionID is NULL
      ) A 
      LEFT JOIN
      pre_installationDF B
      ON(A.SchoolSiteCDSCode= B.cdscode AND A.kWhPurchaseFromUtility=B.pre_peps)""")
      
      pre_test.createOrReplaceTempView("pre_test")
        
         //Post Installation Dataframe
      val post_installationDF=spark.sql("""
      SELECT
      A.cdscode,
      A.kWhPurchaseFromUtility post_peps, 
      SUM(A.intervalreading) post_imd
      FROM
      (
      SELECT
      A.cdscode,
      A.kWhPurchaseFromUtility,
      B.SSID,
      B.CompletionDate,
      B.OLD_CompletionDate,
      B.NEW_CompletionDate,
      C.starttimedaily,
      C.intervalreading
      FROM
      (
		  SELECT 
		  DISTINCT
		  CAST(A.SchoolSiteCDSCode AS BIGINT) cdscode ,
		  A.kWhPurchaseFromUtility,
		  A.SSID
		  FROM 
		  prop39.curated_fsa_tblsiteinformation A
      ) A
      JOIN
      (
		  SELECT 
		  C.SSID,
		  E.CompletionDate,
		  E.OLD_CompletionDate,
		  E.NEW_CompletionDate
		  FROM
		  prop39.curated_fsa_tblSiteSummary C 
		  INNER JOIN 
		  prop39.curated_fsa_tblsubmissions D  
		  ON (C.submissionid =D.submissionid)
		  INNER JOIN
		  (SELECT 
		  E.FiscalYear,
		  E.StatusID,
		  E.ReportingID,
		  E.CompletionDate,
		  DATE_SUB(concat( concat('20',substring(E.CompletionDate, 7, 2)), '-', substring(E.CompletionDate, 1, 2), '-', 
		  substring(E.CompletionDate, 4, 2)), 0) OLD_CompletionDate,
		  DATE_ADD(concat( concat('20',substring(E.CompletionDate, 7, 2)), '-', substring(E.CompletionDate, 1, 2), '-', 
		  substring(E.CompletionDate, 4, 2)), 365) NEW_CompletionDate
		  from  prop39.curated_fsa_tblFinalReporting E ) E 
		  ON(D.ReportingID=E.ReportingID)
      ) B
      ON (A.SSID=B.SSID)
      JOIN
      (
		  SELECT 
		  A.cdscode,
		  A.streetaddress,
		  A.starttimedaily,
		  SUM(A.intervalreadingenergydelivered) intervalreading
		  FROM 
		  prop39.xmlimdtemp A
		  GROUP BY
		  A.cdscode,
		  A.streetaddress,
		  A.starttimedaily
      ) C
      ON(A.cdscode=C.cdscode)
      ) A
      WHERE
      A.starttimedaily>= A.OLD_CompletionDate AND A.starttimedaily<= A.NEW_CompletionDate
      GROUP BY
      A.cdscode, A.kWhPurchaseFromUtility
      """)
        
      post_installationDF.createOrReplaceTempView("post_installationDF")
    	
      val post_test=spark.sql("""select distinct A.schoolsitecdscode,A.kWhPurchaseFromUtility,B.post_peps,B.post_imd,A.submissionid  as finalreporting_submissionid from
      (select fsi.schoolsitecdscode,kWhPurchaseFromUtility,ffr.submissionid
      from prop39.curated_fsa_tblsiteinformation fsi
      join prop39.curated_fsa_tblsitesummary ftss
      on fsi.ssid = ftss.ssid
      join prop39.curated_fsa_tblsubmissions ftsb
      on ftss.submissionid = ftsb.submissionid
      join prop39.curated_fsa_tblfinalreporting ffr 
      on ffr.reportingid = ftsb.reportingid) A
      left join post_installationDF B
      on A.schoolsitecdscode = B.cdscode
      and A.kWhPurchaseFromUtility = b.post_peps""")
      
      post_test.createOrReplaceTempView("post_test")

    	val preAndPostDF=spark.sql("""select  
      distinct pf.CDSCode,pf.pre_peps,pf.pre_imd,
      pt.kWhPurchaseFromUtility as post_peps,pt.post_imd 
      from 
      pre_test pf
      left join 
      post_test pt
      on pf.CDSCode = pt.schoolsitecdscode
      and pf.submissionid = pt.finalreporting_submissionid""")

    
    preAndPostDF.createOrReplaceTempView("preAndPostDF")
    preAndPostDF.printSchema()
 
    //Dropping previous Hive table if exists
    spark.sql("""DROP table if exists prop39.purposebuilt_prop39_imd_peps""")
    
    //Pushing data to Hive internal table
    spark.sql("""create table prop39.purposebuilt_prop39_imd_peps as select * from preAndPostDF""")
  
     //Pushing data to Purposebuilt HDFS path
    preAndPostDF.coalesce(10).write.format("""com.databricks.spark.csv""").option("delimiter", "\t").mode("overwrite").save("""/cec/purposebuilt/nonconf/prop39/pre_post_installation""")
  
    //Metadata Calculations begins here
    val schemaString = "Source,Destination,Size,Row_Count,Time"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    
    val meta_tblSiteInformation = spark.sql("""select * from prop39.curated_tblSiteInformation""")
    val meta_tblimd_xml_conversion = spark.sql("""select * from prop39.curated_tblimd_xml_conversion""")
    val meta_upkeep_selectiontypes = spark.sql("""select * from prop39.curated_upkeep_selectiontypes""")
    val meta_fsa_tblsiteinformation = spark.sql("""select * from prop39.curated_fsa_tblsiteinformation""")
    val meta_fsa_tblSiteSummary = spark.sql("""select * from prop39.curated_fsa_tblSiteSummary""")
    val meta_fsa_tblsubmissions = spark.sql("""select * from prop39.curated_fsa_tblsubmissions""")
    val meta_fsa_tblFinalReporting = spark.sql("""select * from prop39.curated_fsa_tblFinalReporting""")
    
    
    //List of all the files and counts
    val metadata_List = List(Row("Source TableName : prop39.curated_tblSiteInformation", "/cec/purposebuilt/nonconf/prop39/pre&post_installation", "NIL", meta_tblSiteInformation.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_tblimd_xml_conversion", "/cec/purposebuilt/nonconf/prop39/pre&post_installation", "NIL", meta_tblimd_xml_conversion.count().toString(), PresentDate()),
                             Row("Source TableName : prop39.curated_upkeep_selectiontypes", "/cec/purposebuilt/nonconf/prop39/pre&post_installation", "NIL", meta_upkeep_selectiontypes.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_fsa_tblsiteinformation", "/cec/purposebuilt/nonconf/prop39/pre&post_installation", "NIL", meta_fsa_tblsiteinformation.count().toString(), PresentDate()),
                             Row("Source TableName : prop39.curated_fsa_tblSiteSummary", "/cec/purposebuilt/nonconf/prop39/pre&post_installation", "NIL", meta_fsa_tblSiteSummary.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_fsa_tblsubmissions", "/cec/purposebuilt/nonconf/prop39/pre&post_installation", "NIL", meta_fsa_tblsubmissions.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_fsa_tblFinalReporting", "/cec/purposebuilt/nonconf/prop39/pre&post_installation", "NIL", meta_fsa_tblFinalReporting.count().toString(), PresentDate()))
     
     val rdd = spark.sparkContext.makeRDD(metadata_List)
     val finalDF = spark.createDataFrame(rdd, schema)
     
     //Writing the metadata in HDFS path
     val hadoopConf = new org.apache.hadoop.conf.Configuration()
     val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
     finalDF.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp/")
     val fs3 = hdfs.globStatus(new org.apache.hadoop.fs.Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp/part-00000*.csv"));
     hdfs.rename(fs3(0).getPath, new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/meta_preAndPost_"+CurrentTimeStamp()+".csv"))
     hdfs.delete(new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp/"), true)
    
       spark.stop()
  }
  
}