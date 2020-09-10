import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchJob {
  def main(args: Array[String]) {
    // Read the command line arguments to determine the input file and output dir
    val inputFile = if (args.size > 0) args(0) else "../in/routes.dat"
    val outputDir = if (args.size > 1) args(1) else "../out/"

    val spark = SparkSession.builder.appName("Departure Airports Batch Job").getOrCreate()
    
    // Read in the specified input file using the custom schema specified in CustomSchema
    val departureDf = spark.read
        .option("sep", ",")
        .option("inferSchema", "false")
        .option("header", "false")
        .schema(CustomSchema.schema)
        .csv(inputFile)
        .cache()
    
    // Group the data by source airport and count the number of entries per group
    val departureCountsDf = departureDf.groupBy("source_airport").count()
    val mostPopularDeparturesDf = departureCountsDf.orderBy(desc("count")).limit(10)
    
    // Print the table with the 10 most popular departures for debugging purposes 
    mostPopularDeparturesDf.show()
    
    // Write the table to the specified output directory and overwrite previous output
    mostPopularDeparturesDf.write.mode("overwrite").csv(outputDir)
 
    spark.stop()
  }
}
