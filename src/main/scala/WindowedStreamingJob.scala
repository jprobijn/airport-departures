import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WindowedStreamingJob {
  def main(args: Array[String]) {
    // Read the command line arguments to determine the input dir and output dir
    val inputDir = if (args.size > 0) args(0) else "../in/"
    val outputDir = if (args.size > 1) args(1) else "../out/"
    val checkpointDir = "../spark-checkpoints/"

    val spark = SparkSession.builder.appName("Departure Airports Streaming Job").getOrCreate()
    
    // Read in the files in the specified directory as an inputstream, using the custom schema specified in CustomSchema
    val departureStream = spark.readStream
        .option("sep", ",")
        .schema(CustomSchema.schema)
        .csv(inputDir)
    
    // Add processing time timestamps and associated watermarks for time-based windowing
    val departuresWithTimestamp = departureStream
        .withColumn("timestamp", current_timestamp())
        .withWatermark("timestamp", "1 seconds")
           
    // Group the data by time window and source airport and count the number of entries per group
    val departureCountsDf = departuresWithTimestamp
        .groupBy(window(col("timestamp"), "3 seconds"), col("source_airport")).count()
    
    // TODO: Top 10 airports per window
    val mostPopularDeparturesDf = departureCountsDf
    
    // Split the window column into a separate start and end column for clean formatting
    val mostPopularDeparturesFormattedDf = mostPopularDeparturesDf
        .select("window.start", "window.end", "source_airport", "count")

    // Print the table with the 10 most popular departures for debugging purposes
    // Ideally, we'd write to a file, but without time based aggregations we can't do this
    val consoleQuery = mostPopularDeparturesFormattedDf.writeStream
        .outputMode("complete")
        .format("console")
        .start()

    // Write the results to files in the specified output directory and overwrite previous output
    val outputQuery = mostPopularDeparturesFormattedDf.writeStream
        .outputMode("append")
        .format("csv")
        .option("checkpointLocation", checkpointDir)
        .option("path", outputDir)
        .option("mode", "overwrite")
        .start()
    
    consoleQuery.awaitTermination()
    outputQuery.awaitTermination()
    spark.stop()
  }
}
