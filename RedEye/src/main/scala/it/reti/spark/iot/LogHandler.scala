package it.reti.spark.iot
import org.apache.log4j.Logger 
import org.apache.log4j.Level


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This object sets app log levels and gives access to a transient reference to Logger methods in order 
 * to permit cluster distributed logging operations
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
object LogHandler extends Serializable {
  

  
  /*--------------------------------------------------
  * SETTING LOG LEVELS
  *---------------------------------------------------*/
  Logger.getRootLogger.setLevel(Level.WARN)
  
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("it.reti.spark.iot").setLevel(Level.INFO)
  Logger.getLogger("it.reti.spark.iot.Main").setLevel(Level.INFO)
  Logger.getLogger("it.reti.spark.iot.SocketReceiver").setLevel(Level.INFO)
  
  
  
  
  
  
  
  
  
  
  
  
  //defining a lazy log variable
  @transient private lazy val myLog = Logger.getLogger(getClass.getName)
  
  
  
  
  //.................................................................................................................
  /**
   * method to 
   * @return myLog lazy variable
   */
  def log = myLog
 
  
  
  
}// end LogHandler class //