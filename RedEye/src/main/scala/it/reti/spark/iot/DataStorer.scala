package it.reti.spark.iot


import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.hbase._





/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
 * 
 */
class DataStorer {
  
  
  val tableName = "iot_avg_data"
  
  
  /* ---------------------------------------------------------------------------------------------------------------/
   * HBASE CONFIGURATION:
   * a catalog variable which contains all needed infos about table-name, column families
   * and column-names.
   * It is the key part used by the connector to build a DataFrame bridge between Spark and
   * HBase.
   * 
   * 
   * It consists of defining a string with a nested mapping as follows:
   * 
   * "table": {namespace, tablename},
   * "rowkey": key-column-name,  					!!Base MUST need a single-column primary-key!!
   * "columns"{ 
   * 						list of:
   * 						"SPARK-DataFrame-ColumnName": {"cf": HBase-ColumnFamily, "col": HBase-ColumnName, "type": format }
   * 					}
   * 
  / ----------------------------------------------------------------------------------------------------------------*/

  def catalog = s"""{
                |"table":{"namespace": "default", "name": "$tableName"},
                |"rowkey":"key",
                |"columns":{
                            |"timestamp":{"cf": "rowkey",  "col":"key",         "type":"string"},
                            |"distanza":{"cf": "distanza", "col":"distanza",    "type":"string"},
                            |"luminosita1":{"cf": "luminosita1", "col":"luminosita1", "type":"string"},
                            |"luminosita2":{"cf": "luminosita2", "col":"luminosita2", "type":"string"},
                            |"luminosita3":{"cf": "luminosita3", "col":"luminosita3", "type":"string"},
                            |"temperatura":{"cf": "temperatura", "col":"temperatura", "type":"string"}
                            |}
                |}""".stripMargin
  
  
  
  
  
  
  
  
  
  
  
  
  /*..................................................................................................................*/
  /**
   *
   */
  def storeIntoHBase (sensorDF: DataFrame) = {

    
    val sqlContext = ContextHandler.getSqlContext
    import sqlContext.implicits._
    
  /*
   * In order to write into HBase using the connector you have to use the standard write DataFrame interface
   * providing through the "option" method:
   * - the catalog defined above
   * - the number of Region into which the HBase table must be split when a new creation occurs
   *   (this option is ignored if the table already exists)
   *   
   * and specifying the shc writing format.
   *    
   */
    sensorDF.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
                  .format("org.apache.spark.sql.execution.datasources.hbase")
                  .save()
    
    
  }//end storeIntoHBase method //
  
  
  
  
  
  
}//end DataStorer class //