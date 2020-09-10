import org.apache.spark.sql.types._

object CustomSchema {
    // Schema matching the OpenFlights routes data
    val schema = StructType(
        StructField("airline", StringType, false) ::
        StructField("airline_id", StringType, false) ::
        StructField("source_airport", StringType, false) ::
        StructField("source_airport_id", StringType, false) ::
        StructField("dest_airport", StringType, false) ::
        StructField("dest_airport_id", StringType, false) ::
        StructField("codeshare", StringType, true) ::
        StructField("stops", IntegerType, false) ::
        StructField("equipment", StringType, false) :: Nil)
}
