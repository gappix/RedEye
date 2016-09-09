package it.reti.spark.iot

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders

/*########################################## THIS IS THE MAIN START ##################################################*/
/**
 *
 */
object Main extends Logging{
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Main <port>")
      System.exit(1)
    }
    val Array(port) = args
    
     
    //welcome title printed
    Title.printTitle

    val logHandler =  LogHandler
    val elaborator = new DataElaborator
    val storer = new DataStorer
    //opening context
    val ssc = new StreamingContext(ContextHandler.getSparkContext, Seconds(5))
    
    
    
    /*------------------------------------------------------------------------------------------
     * Receiving data stream
     *-----------------------------------------------------------------------------------------*/
    val myReceiver = new SocketReceiver(port.toInt)
    // Create a DStream that will open a port connection on the specified port
    val lines = ssc.receiverStream(myReceiver)
    
    
    
    
    /*------------------------------------------------------------------------------------------
     * Each RDD transformations
     *-----------------------------------------------------------------------------------------*/
      lines.foreachRDD( rdd => {
         
      
                         //transform json-format bunch of data into a DataFrame
                         val sqlContext = ContextHandler.getSqlContextHIVE
                         import sqlContext.implicits._
                         
                         
                         val dataDF  = sqlContext.read.json(rdd).persist()
                         
                  
                         
                         dataDF.printSchema()
                         
                         dataDF.show()
                         /*<<< INFO >>>*/ logDebug("Received " + dataDF.count() + " sensor data")
                         
                         
                         
                         //if there is any data -> elaborate and store!
                         if (dataDF.count() != 0){

                                 //send DataFrame to average method elaborator
                                 val averagedDataDF = elaborator.computeAvgValues(dataDF)
                                 
                                 //show results
                                 averagedDataDF.show()
                                 
                                 //send DataFrame to storer method
                                storer.storeIntoHBase(averagedDataDF)

                         }//enf if




                         dataDF.unpersist()
                         
                         
                     })//end foreachRDD
    
    

    
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
    

    
    
    
  
  }//end main method //



}//end Main object //
