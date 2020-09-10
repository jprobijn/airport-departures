import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingJob {
  def main(args: Array[String]) {
    // Read the command line arguments to determine the input dir
    val inputDir = if (args.size > 0) args(0) else "../in/"

    val spark = SparkSession.builder.appName("Departure Airports Streaming Job").getOrCreate()
    
    // Read in the files in the specified directory as an inputstream, using the custom schema specified in CustomSchema
    val departureStream = spark.readStream
        .option("sep", ",")
        .schema(CustomSchema.schema)
        .csv(inputDir)
    
    // Group the data by source airport and count the number of entries per group
    val departureCountsDf = departureStream.groupBy("source_airport").count()
    val mostPopularDeparturesDf = departureCountsDf.orderBy(desc("count")).limit(10)
    
    // Print the table with the 10 most popular departures for debugging purposes
    // Ideally, we'd write to a file, but without time based aggregations we can't do this
    val query = mostPopularDeparturesDf.writeStream
        .outputMode("complete")
        .format("console")
        .start()
    
    query.awaitTermination()
    spark.stop()
  }
}
