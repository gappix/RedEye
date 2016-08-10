package it.reti.spark.iot

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.hbase





/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
 * 
 */
class DataStorer {
  
  
  val tableName = "iot_avg_data"
  
  
  /* -------------------------------------/
   * HBase configuration
  / --------------------------------------*/
  val confHBase = HBaseConfiguration.create()
  confHBase.set(TableOutputFormat.OUTPUT_TABLE, tableName)
  val jobConfig = new JobConf(confHBase, this.getClass)
  jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
  jobConfig.setOutputFormat(classOf[TableOutputFormat])
  jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
  
  
  
  
  
  
  
  
  
  
  
  
  
  /*..................................................................................................................*/
  /**
   *
   */
  def storeIntoHBase (sensorRDD: RDD[SensorData]) = {

  // sensorRDD.map(Sensor.convertToPut()).saveAsHadoopDataset(jobConfig)
    
  }//end storeIntoHBase method //
  
  
  
  
  
  
}//end DataStorer class //