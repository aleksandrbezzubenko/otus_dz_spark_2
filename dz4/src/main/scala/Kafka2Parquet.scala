import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

object Kafka2Parquet extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("PostgresWriterJob")
    .getOrCreate()

  val userSchema = new StructType()
    .add("Name", "string")
    .add("Author", "string")
    .add("Rating", "double")
    .add("Reviews", "long")
    .add("Price", "long")
    .add("Year", "integer")
    .add("Genre", "string")

  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("topic", "test")
    .load()
    .selectExpr("CAST(value as STRING)")
    .withColumn("jsonData", explode(from_json(col("value"), schema)))
    .select("jsonData.*")
    .filter(col("Rating") >= 4)
    .coalesce(1)
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "parquet_test")
    .option("checkpointLocation", "checkDir2")
    .start()


}
