package it.reti.spark.iot

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode



/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
 * 
 */
class DataStorer {
  
  
  val tableName = "iot_avg_data"
  
  
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion 
  private val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
  sqlContextHIVE.sql("CREATE TABLE IF NOT EXISTS " + tableName + " (timestamp double, distanza float, luminosita1 float, luminosita2 float, luminosita3 float, temperatura float)  STORED AS ORC")
  
  
  
  
  
  
  /*................................................................................................................*/
  /**
   * 
   */
  def storeDFtoHIVE (dataDF: DataFrame) = {
    
    
    dataDF.rdd.saveAsTextFile("/usr/maria_dev/RedEye/tryoutSave.txt")
    //.write.mode(SaveMode.Append).format("orc").insertInto(tableName)
    
    
  }//end storeDFtoHIVE method //
  
  
  
  
  
  
}//end DataStorer class //