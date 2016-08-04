package it.reti.spark.iot


import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders

case class SensorData(  distance: Double, brightness1: Double, brightness2: Double, brightness3: Double, temperature: Double )



object Main extends Logging{
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Main <port>")
      System.exit(1)
    }
    val Array(port) = args
    
      
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
    val linesDF =    lines.foreachRDD( rdd => {
         
      
                         //transform json-format bunch of data into a DataFrame
                         val sqlContext = ContextHandler.getSqlContextHIVE
                         import sqlContext.implicits._
                         
                         
                         val dataDF  = sqlContext.read.json(rdd)
                         
                  
                         
                         
                         
                         dataDF.show()
                         /*<<< INFO >>>*/ logDebug("Received " + dataDF.count.toString() + " sensor data")
                         
                         
                         
                         //if there is any data -> elaborate and store!
                         if (dataDF.count() != 0){
                             
                           
                             //send DataFrame to average method elaborator
                             val averagedDataDF = elaborator.computeAvgValues(dataDF)
                             averagedDataDF.show()
                             
                             
                             //send DataFrame to storer method
                             /*<<< INFO >>>*/  logInfo("Storing data into HIVE.......")
                             storer.storeDFtoHIVE(averagedDataDF)
                             /*<<< INFO >>>*/  logInfo("Data stored!")
                             
                         }
                         
                        
                     })//end foreachRDD
    
    

    
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
    
    
    /*graceful stop 
    sys.ShutdownHookThread {   
      
        /*++ WARN ++*/ logWarning("Gracefully stopping Spark Streaming Application")
        
        myReceiver.onStop()
        ssc.stop(true, true)
        
        /*++ WARN ++*/  logWarning("Application gracefully stopped")
        
    }*/
    
    
    
  
  }//end main method //



}//end Main object //
