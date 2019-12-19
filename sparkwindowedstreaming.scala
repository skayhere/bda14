package com.nmit.spark.sparkwindowedstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{window, col}
import org.apache.spark.sql.types.StructType

object sparkWindowedStreaming {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("sparkWindowedStreaming").getOrCreate()
  
    import spark.implicits._

    spark.conf.set("spark.sql.shuffle.partitions", 5)

    val userSchema = new StructType().
      add("Creation_Time", "double").
      add("Station", "string").
      add("Rainfall", "float")

    val streaming = spark.
      readStream.
      schema(userSchema).
      json("/home/subhrajit/sparkProjects/data/event-data/threeWindows")

    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    val events_per_window = withEventTime
      .groupBy(window(col("event_time"), "15 minutes"))
      .agg(avg("Rainfall"),count("Station"))
      .writeStream
      .queryName("events_per_window")
      .format("console")
      .outputMode("complete")
      .option("truncate", false)
      .start()

    events_per_window.awaitTermination()

  }
}

// .withWatermark("event_time", "1 minutes")
// .groupBy(window(col("event_time"), "15 minutes", "5 minutes"))

// val events_per_window = withEventTime
    // .agg(avg("Rainfall"))
    // .writeStream
    // .queryName("events_per_window")
    // .format("console")
    // .outputMode("complete")
    // .option("truncate", false)
    // .start()


