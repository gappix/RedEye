package it.reti.spark.iot

import org.apache.spark.Logging
import org.apache.phoenix.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object PhoenixTest extends Logging{
  
  
  
  
  def main(args: Array[String]) {


val sc = new SparkContext("local", "phoenix-test")
val sqlContext = new SQLContext(sc)

val df = sqlContext.load(
  "org.apache.phoenix.spark",
  Map("table" -> "TABLE1", "zkUrl" -> "10.1.2.28:2181")
)
df
  .filter(df("COL1") === "test_row_1" && df("ID") === 1L)
  .select(df("ID"))
  .show

    
    
  }
  
  
}