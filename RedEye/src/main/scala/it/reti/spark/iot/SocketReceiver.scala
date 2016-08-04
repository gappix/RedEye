package it.reti.spark.iot

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.Socket

class SocketReceiver(port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  var serverSocket : ServerSocket = null
  var socket : Socket = null
  
  
  def onStart() {
    serverSocket = new ServerSocket(port);
    
    
    /*<<< INFO >>>*/ //logInfo("Created serverSocket: "+ serverSocket.toString())
    
    
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { while(!isStopped) receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
    socket.close()
    socket = null
    serverSocket.close()
    serverSocket = null
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
   
      
          
          try {
        	      var userInput: String = null   
        			  socket = serverSocket.accept()
        			  /*##DEBUG##*/ logDebug("Accepted socket: "+ socket.toString())
        			  
            
               // Until stopped or connection broken continue reading
               val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
               /*-- TRACE --*/ logTrace(">>> Opened new bufferReader: "+ reader.toString())
              
               
               userInput = reader.readLine()
               
               
               
               while( userInput != null) {
                 
      
                      /*--TRACE --*/ logTrace(">>> UserInput: "+ userInput.toString())     
                      store(userInput)
                      userInput = reader.readLine()
                 
               }
               
               /*--TRACE--*/ logTrace(">>> BufferReader " + reader.toString() + "chiuso")
               socket.close()
               /*--TRACE--*/ logTrace(">>> Socket " + socket.toString() + "chiusa")
            
      
          } catch {
           case e: java.net.ConnectException =>
             // restart if could not connect to server
             /*!!ERROR!!*/ logError("#Error connecting to port " + port, e)
             restart("Error connecting to port " + port, e)
           case t: Throwable =>
             // restart if there is any other error
             /*!!ERROR!!*/logError("#Error receiving data", t)
             restart("Error receiving data", t)
          }
          
    

    
    
    
    
  }//end receive method //
  
  
  
}//end SocketReceiver class //