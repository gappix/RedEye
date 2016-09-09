package it.reti.spark.iot

import org.apache.commons.net.ntp.TimeStamp
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes



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














