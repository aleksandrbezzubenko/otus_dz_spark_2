import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

object Main extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("PostgresWriterJob")
    .getOrCreate()

  val userSchema = new StructType()
    .add("Name", "string")
    .add("Author", "string")
    .add("User Rating", "double")
    .add("Reviews", "long")
    .add("Price", "long")
    .add("Year", "integer")
    .add("Genre", "string")

  import spark.implicits._
  spark.readStream
    .format("csv")
    .schema(userSchema)
    .load("src/main/resources/bestsellers.csv")
     .withColumnRenamed("User Rating", "rating")
    .as[Book]
    .toJSON
    .selectExpr("CAST(value as STRING)")
    .writeStream
    .outputMode("append")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("topic", "test")
    .option("checkpointLocation", "checkDir")
    .start()

  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("topic", "test")
    .load()
    .filter(col("rating") >= 4.0)
    .coalesce(1)
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "parquet_test")
    .option("checkpointLocation", "checkDir2")
    .start()


}
