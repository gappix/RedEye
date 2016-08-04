package it.reti.spark.iot

//imports needed
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This object instantiates all SPARK Contexts once, and then retrieves them with appropriate methods
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
object ContextHandler extends Logging{
  
  
  
  /*<<< INFO >>>*/ logInfo("creating SPARK Contexts...")
  
  //app hard configurations
  private val conf = new SparkConf()
    .setAppName("SPARK IoT Experiment")
    .setMaster("local[*]")
    //Kryo Options
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses( Array(
    classOf[scala.collection.mutable.WrappedArray$ofRef],
    classOf[Array[org.apache.spark.streaming.receiver.Receiver[_]]],
    classOf[SocketReceiver],
    classOf[org.apache.spark.sql.types.StructType],
    classOf[Array[org.apache.spark.sql.types.StructField]],
    classOf[org.apache.spark.sql.types.StructField],
    classOf[org.apache.spark.sql.types.StringType],
    classOf[Array[org.apache.spark.sql.catalyst.InternalRow]],
    classOf[org.apache.spark.sql.types.StringType$],
    classOf[org.apache.spark.sql.types.Metadata],
    classOf[scala.collection.immutable.Map$EmptyMap$],
    classOf[org.apache.spark.sql.catalyst.expressions.UnsafeRow],
    classOf[org.apache.spark.sql.catalyst.expressions.GenericInternalRow],
    classOf[Array[Object]],
    classOf[org.apache.spark.unsafe.types.UTF8String]
    ))
    .set("spark.kryo.registrationRequired","true")
    //Spark logger options
    .set("spark.eventLog.enabled","true")
    .set("spark.eventLog.compress","true")


  //SPARK contexts creation
  private val sc = new SparkContext(conf)
  private val sqlContext = new SQLContext(sc)
  private val sqlContextHIVE = new HiveContext(sc)
  
  private val status = "ok"
   
  
  /*<<< INFO >>>*/ logInfo("Contexts created!")
  
  
  
  
  //....................................................................................................................
  /**
   * method to instantiate object and check if successful
   * @return string status
   */
  def setAllContexts = status
  
  
  
  //....................................................................................................................
  /**
   * method that
   * @return active SparkContext
   */
  def getSparkContext = sc
  
  
  
  //....................................................................................................................
  /**
   * method that
   * @return active sqlContext
   */
  def getSqlContext = sqlContext
  
  
  
  //....................................................................................................................
  /**
   * method that
   * @return active Hive context
   */
  def getSqlContextHIVE = sqlContextHIVE
  
  
 
  
  
  
  
}//end ContextHandler object //