package it.reti.spark.iot

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.{Row,DataFrame}
import org.apache.spark.sql.functions._


/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
 * 
 */
class DataElaborator {
  
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion 
  private val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
  
  
  /*---------------------------------------------------------------------------------
   * UDF definition: function needed for desired dataframe selection
   *---------------------------------------------------------------------------------*/

  //function for unboxing bounding box structure in order to get Place Latitude information
  val timeStamper = udf((something: Float) => {
    System.currentTimeMillis()
  })
    
 
  
  
  
  
  /*................................................................................................................*/
  /**
   * 
   */
  def computeAvgValues(dataDF: DataFrame): DataFrame = {
    

    //computing average of all data fields
    val averagedDataDF = dataDF.select( max(timeStamper(dataDF("distanza"))).as("timestamp"),
                                        avg($"distanza").as("distanza"),
                                        avg($"luminosita1").as("luminosita1"),
                                        avg($"luminosita2").as("luminosita2"),
                                        avg($"luminosita3").as("luminosita3"),
                                        avg($"temperatura").as("temperatura")                                        
                                        )
    
    //return DataFrame
    averagedDataDF
    
    
  }//end computeAvgValues method //         


  
}