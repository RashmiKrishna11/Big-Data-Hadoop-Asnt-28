package MLIB

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object asnt27 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("MLIB example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    println("Spark Session Object created")

    val Flight = spark.read.format("CSV").option("header", true).load("E:\\assignments ss\\DelayedFlights.csv")
    val Fl = Flight.toDF()
   // Flight.show()
    Fl.registerTempTable("Flights_Table")
    println("Flights_Table is registered!")
    //Find out the top 5 most visited destinations.
    val dest = spark.sql("""select Dest,count(dest) as Visits from Flights_Table  group by Dest """).toDF()
     dest.sort(desc("Visits")).show(5)
    println("Top 5 most visited destinations are as above!")

    //Which month has seen the most number of cancellations due to bad weather?
    val cancel = spark.sql("""select Month,count(FlightNum) as Num_Flights_Cancelled from Flights_Table WHERE Cancelled =1 AND CancellationCode="B" group by Month """).toDF()
   cancel.show()
    println("Month which has seen the most of the number of cancellations due to bad weather are as above!")

    //Which route (origin & destination) has seen the maximum diversion?
    val diversion = spark.sql("""select Origin,Dest,count(FlightNum) as Max_Diversion from Flights_Table where Diverted =1 group by Origin,Dest """)
    diversion.toDF().sort(desc("Max_Diversion")).show()
    println("List of Origin and destination that has seen the maximum diversion")
  }
}