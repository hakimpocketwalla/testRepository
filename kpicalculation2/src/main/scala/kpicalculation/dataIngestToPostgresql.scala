package kpicalculation

import org.apache.spark.sql.SparkSession

object dataIngestToPostgresql {
  
  def main(args: Array[String]) = {
    
val warehouseLocation = "hdfs://california-dwr-hdfs/apps/hive/warehouse/"

val spark = SparkSession
  .builder()
  .appName("CEC_dataIngestToPostgre")
  .config("spark.master", "yarn")
  .config("spark.sql.warehouse.dir", warehouseLocation)
  .config("hive.metastore.uris", "thrift://hdp-pr-master-0004.ad.water.ca.gov:9083")
  .config("spark.ui.port", "44040" )
  .config("spark.driver.allowMultipleContexts", "true")
  .enableHiveSupport()
  .getOrCreate()
  
	import spark.implicits._
    	  
    
  //Code Starts
	
	//create properties object
val prop = new java.util.Properties
prop.setProperty("driver", "org.postgresql.Driver")
prop.setProperty("user", "accenture")
prop.setProperty("password", "ces_dbms") 
 
//jdbc mysql url - destination database is named "accenture"
val url = "jdbc:postgresql://hdp-tool-postgrsql-0001:5432/accenture"
 
//destination database table 
val table1 = "prop39_program_funding_summary"
val table2 = "prop39_measure_details"
val table3 = "prop39_imd_peps"
 
//write data from spark Hive database to dataframe 
val df1 = spark.sql("""select * from prop39.purposebuilt_funding_summary""")
val df2 = spark.sql("""select * from prop39.purposebuilt_measure_details""")
val df3 = spark.sql("""select * from prop39.purposebuilt_prop39_imd_peps""")

//write data from spark dataframe to RDBMS database
df1.write.mode("overwrite").jdbc(url, table1, prop)
df2.write.mode("append").jdbc(url, table2, prop)
df3.write.mode("overwrite").jdbc(url, table3, prop)

}
  
}