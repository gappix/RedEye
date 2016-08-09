package it.reti.spark.iot

import org.apache.commons.net.ntp.TimeStamp

/*
 
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

*/


/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
  *
  *
  */
case class  SensorData(  distanza: Double, luminosita1: Double, luminosita2: Double, luminosita3: Double, temperatura: Double )










/*++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
 *
 *
 */
case class  SensorDataAVG(  timestamp: TimeStamp, distance: Double, brightness1: Double, brightness2: Double, brightness3: Double, temperature: Double)












/*+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*/
/**
 *
 */
object Sensor extends Serializable {



  /*
   * Non sono sicuro di come funzioni.....
   */
  
  
  //val dataBytes = Bytes.toBytes("data")




  /*..................................................................................................................*/
  /**
   *
   * @param sensor rdd to convert to bytes for an HBase storage
   * @return tuple(ImmutablesBytesWritable(rowkey) | put row alias)
   */
  /*
  def convertToPut(sensor: SensorDataAVG): (ImmutableBytesWritable, Put) = {


    val rowkey = sensor.distance

    val put = new Put(Bytes.toBytes(rowkey)) // add to column family data, column  data values to put object
    put.add(dataBytes, Bytes.toBytes("hz"), Bytes.toBytes(sensor.distance))
    put.add(dataBytes, Bytes.toBytes("disp"), Bytes.toBytes(sensor.brightness1))
    put.add(dataBytes, Bytes.toBytes("flo"), Bytes.toBytes(sensor.brightness2))
    put.add(dataBytes, Bytes.toBytes("sedPPM"), Bytes.toBytes(sensor.brightness3))
    put.add(dataBytes, Bytes.toBytes("psi"), Bytes.toBytes(sensor.temperature))



    (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)

  } //end convertToPut method //

*/

}//end SensorObject
