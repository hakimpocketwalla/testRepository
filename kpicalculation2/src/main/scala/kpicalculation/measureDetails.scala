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

object measureDetails {
 
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
  .appName("CEC_measureDetails")
  .config("spark.master", "yarn")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .config("hive.metastore.uris", "thrift://hdp-pr-master-0004.ad.water.ca.gov:9083")
  .config("spark.ui.port", "44040" )
  .config("spark.driver.allowMultipleContexts", "true")
  .enableHiveSupport()
  .getOrCreate()
  
    import spark.implicits._
    
    spark.sql("""LOAD DATA INPATH 'hdfs://california-dwr-hdfs/cec/ed/raw/nonconf/peps/siteinformation/asa/asa_tblSiteInformation_02052018.csv' OVERWRITE INTO TABLE default.asa_tblSiteInformation""")
    
  
  //Code Starts
  
    val tblSiteinformationDF = spark.sql("""select distinct localeducationagencyleaname, schoolsitename, schoolsitemailingaddress, city, zipcode, schoolsitecdscode, estimatedprojectstartdate,proposition39sharetobeusedformeasureimplementation, ssid, siid from prop39.curated_tblSiteinformation""")
    val tblsitesummaryDF = spark.sql("""select distinct leacdscode,ssid, submissionid from  prop39.curated_tblsitesummary""")
    
    val join1DF = tblSiteinformationDF.join(tblsitesummaryDF,Seq("ssid"))
    val tblsubmissionsDF = spark.sql("""select distinct submissionid, statusid FROM prop39.curated_tblsubmissions WHERE (statusid=5 or statusid=10) and (aaasubmissionid is null or aaasubmissionid='')""")
    
    val join2DF = join1DF.join(tblsubmissionsDF,Seq("submissionid"))
    val tblleacodesDF = spark.sql("""select distinct county_code, county_name, lea_code_cds_ as lea_code_cds, lea_type__charter_districts_in_yellow_shading_ as lea_type ,tier FROM prop39.curated_tblleacodes""")
    
    val join3DF = join2DF.join(tblleacodesDF,join2DF("""leacdscode""") === tblleacodesDF("""lea_code_cds"""))
    val tblelse_reportDF = spark.sql("""select distinct state_name__public_school__latest_available_year as state_name, school_name__public_school__2014_15 as school_name,latitude__public_school__2014_15 as latitude, longitude__public_school__2014_15 as longitude,school_cds_code FROM prop39.curated_tblelse_report""")
    
    val join4DF = join3DF.join(tblelse_reportDF,join3DF("""schoolsitecdscode""") === tblelse_reportDF("""SCHOOL_CDS_CODE"""))
    val tblUtilityStatusTypeDF = spark.sql("""select distinct statusdescription,statusid FROM prop39.curated_tblUtilityStatusType""")
    
    val join5DF = join4DF.join(tblUtilityStatusTypeDF,Seq("statusid"))
    
    //selectiondescription to get measure count and measure cost.
    val hvac_measure = spark.sql("""select selectiondescription from prop39.curated_technologysummarytable where visualization_measure_category='HVAC'""")
    					.collect().map{row=>"'"+row.mkString(",")+"'"}.mkString(",").toString()
    
    val lighting_measure = spark.sql("""select selectiondescription from prop39.curated_technologysummarytable where visualization_measure_category='Lighting'""")
    					.collect().map{row=>"'"+row.mkString(",")+"'"}.mkString(",").toString()
    
    val hvac_lighting_control_measure = spark.sql("""select selectiondescription from prop39.curated_technologysummarytable where visualization_measure_category='HVAC & Lighting Controls'""")
    					.collect().map{row=>"'"+row.mkString(",")+"'"}.mkString(",").toString()
    					
    val other_ee_measure = spark.sql("""select selectiondescription from prop39.curated_technologysummarytable where visualization_measure_category='Other EE Measures'""")
    					.collect().map{row=>"'"+row.mkString(",")+"'"}.mkString(",").toString()

	val comm_rcomm_meaure = spark.sql("""select selectiondescription from prop39.curated_technologysummarytable where visualization_measure_category='Commissioning and Retrocommissioning'""")
    					.collect().map{row=>"'"+row.mkString(",")+"'"}.mkString(",").toString()
	    					
    //Calculating aggregated measure count and mesaure cost for tblEfficiencyMeasure
    val tblEfficiencyMeasureDF = spark.sql("""
	select siid,
	    sum((if(selectiondescription in ("""+hvac_measure+"""),1,null))
		) as hvac_measure, 
		sum(if(selectiondescription in ("""+lighting_measure+"""),1,null)
		) as lighting_measure,
		sum(if(selectiondescription in ("""+hvac_lighting_control_measure+"""),1,null)
		) as hvac_lighting_control_measure,
		sum(if(selectiondescription in ("""+other_ee_measure+"""),1,null)
		) as other_ee_measure,
		sum(if(selectiondescription in ("""+comm_rcomm_meaure+"""),1,null)
		) as comm_rcomm_meaure,
		sum (if( selectiondescription in ("""+hvac_measure+"""),e.measurecost,null)
		) as hvac_measurecost,
		sum(if( selectiondescription in ("""+lighting_measure+"""),e.measurecost,null)
		) as lighting_measurecost,
		sum(if(selectiondescription in ("""+hvac_lighting_control_measure+"""),e.measurecost,null)
		) as hvac_lighting_control_measurecost,
		sum(if(selectiondescription in ("""+other_ee_measure+"""),e.measurecost,null)
		) as other_ee_measurecost,
		sum(if(selectiondescription in ("""+comm_rcomm_meaure+"""),e.measurecost,null)
		) as comm_rcomm_meaurecost
	from prop39.curated_tblefficiencymeasure e
	join prop39.curated_upkeep_selectiontypes us on e.energyefficiencymeasure = us.selectionid
	group by e.siid
    """)
    
    //Calculating aggregated measure count and mesaure cost for tblPhotovoltaicMeasure
    val tblPhotovoltaicMeasureDF = spark.sql("""select siid,
    (count(siid)) as photovoltaic_measure,
    sum(measurecost) as photovoltaic_measurecost 
    from 
    prop39.curated_tblphotovoltaicmeasure
    group by siid""")
    
    val join6DF = tblEfficiencyMeasureDF.join(tblPhotovoltaicMeasureDF, Seq("siid"),"left")
    
    val measure_detailsDF = join5DF.join(join6DF,Seq("siid"))
    
    measure_detailsDF.printSchema()
    measure_detailsDF.createOrReplaceTempView("measure_detailsDF")
    
    //Dropping previous Hive table if exists
    spark.sql("""drop table if exists prop39.purposebuilt_measure_details""")
    
    //Pushing data to Hive internal table
    spark.sql("""create table prop39.purposebuilt_measure_details as select distinct * from measure_detailsDF""")
    
    //Pushing data to Purposebuilt HDFS path
    measure_detailsDF.coalesce(10).write.format("""com.databricks.spark.csv""").option("delimiter", "\t").mode("overwrite").save("""/cec/purposebuilt/nonconf/prop39/measure_details""")
    
    
    
    //Dropping previous Hive table if exists
    spark.sql("""drop table if exists prop39.curated_sitemaster""")
 
	//Pushing data to Hive internal table
    spark.sql("""
                CREATE TABLE prop39.curated_sitemaster
                AS
                SELECT assembly.siid AS siid,
                  CASE
                      WHEN assembly.assembly = 'y' THEN 'y'
                      ELSE 'n'
                  END AS assembly,
                  CASE
                      WHEN senate.senate = 'y'  THEN 'y'
                      ELSE 'n'
                END AS senate,
                CASE
                      WHEN congressional.congress = 'y'  THEN 'y'
                      ELSE 'n'
                END AS congress,
                assembly.districtnumber AS assemblynumber,
                senate.districtnumber AS senatenumber
                FROM (
                      SELECT si.siid,
                                  CASE
                                      WHEN dt.district = 'Assembly' THEN 'y'
                                      ELSE 'n'
                                  END AS assembly,
                              d.districtnumber
                      FROM prop39.curated_tblsiteinformation si
                               JOIN prop39.curated_tbldistricts d ON si.siid = d.siid AND d.districtid = 1
                               JOIN prop39.curated_upkeep_DistrictTypes dt ON d.districtid = dt.districtid
                      ) assembly
                 LEFT JOIN (
                              SELECT si.siid,
                            CASE
                                WHEN dt.district  = 'Senate'  THEN 'y'
                                ELSE 'n'
                            END AS senate,
                        d.districtnumber
                FROM prop39.curated_tblsiteinformation si
                         JOIN prop39.curated_tbldistricts d ON si.siid = d.siid AND d.districtid = 2
                         JOIN prop39.curated_upkeep_DistrictTypes dt ON d.districtid = dt.districtid
                ) senate ON assembly.siid = senate.siid
                LEFT JOIN (
                SELECT si.siid,
                            CASE
                                WHEN dt.district  = 'Congressional'  THEN 'y'
                                ELSE 'n'
                            END AS congress
                FROM prop39.curated_tblsiteinformation si
                         JOIN prop39.curated_tbldistricts d ON si.siid = d.siid AND d.districtid = 3
                         JOIN prop39.curated_upkeep_DistrictTypes dt ON d.districtid = dt.districtid
                ) congressional ON assembly.siid = congressional.siid
            ORDER BY siid""")
    
    val sitemasterDF = spark.sql("""select * from prop39.curated_sitemaster""")
    
    sitemasterDF.coalesce(10).write.format("""com.databricks.spark.csv""").option("delimiter", "\t").mode("overwrite").save("""/cec/purposebuilt/nonconf/prop39/sitemaster""")
    
    //Metadata Calculations begins here
    val schemaString = "Source,Destination,Size,Row_Count,Time"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    //List of all the files and counts
    val metadata_List = List(Row("Source TableName : prop39.curated_tblSiteinformation", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblSiteinformationDF.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_tblsitesummary", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblsitesummaryDF.count().toString(), PresentDate()),
                             Row("Source TableName : prop39.curated_tblsubmissions", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblsubmissionsDF.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_tblleacodes", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblleacodesDF.count().toString(), PresentDate()),
                             Row("Source TableName : prop39.curated_tblelse_report", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblelse_reportDF.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_tblUtilityStatusType", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblUtilityStatusTypeDF.count().toString(), PresentDate()),
                             Row("Source TableName : prop39.curated_tblefficiencymeasure", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblEfficiencyMeasureDF.count().toString(), PresentDate()), 
                             Row("Source TableName : prop39.curated_tblphotovoltaicmeasure", "/cec/purposebuilt/nonconf/prop39/measure_details", "NIL", tblPhotovoltaicMeasureDF.count().toString(), PresentDate()),
     						 Row("Source TableName : prop39.curated_sitemaster", "/cec/purposebuilt/nonconf/prop39/sitemaster", "NIL", sitemasterDF.count().toString(), PresentDate()))
     						 	
     val rdd = spark.sparkContext.makeRDD(metadata_List)
     val finalDF = spark.createDataFrame(rdd, schema)
    
     //Writing the metadata in HDFS path
     val hadoopConf = new org.apache.hadoop.conf.Configuration()
     val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
     finalDF.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp")
     val fs3 = hdfs.globStatus(new org.apache.hadoop.fs.Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp/part-00000*.csv"));
     hdfs.rename(fs3(0).getPath, new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/meta_MeasureDetail_"+CurrentTimeStamp()+".csv"))
     hdfs.delete(new Path("/cec/purposebuilt/nonconf/prop39/metadata/temp/temp/"), true)
    
    spark.stop()
  }
  
  
}