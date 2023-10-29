import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Functions extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("SparkMainApp")
    .master("local")
    .getOrCreate

  import spark.implicits._

  val countriesDF = spark
    .read
    .format("json")
    .load("src/test/resources/countries.json")

  def getCountriesInfo(df: DataFrame): DataFrame = {

    val cca3toCountries = df
      .select("cca3", "name.official")
      .collect()
      .map(row => row(0) -> row(1)).toMap.asInstanceOf[Map[String, String]]

    val countriesToString = (cca3List: Seq[String]) => {
      cca3List.map(x => cca3toCountries(x)).mkString(sep=", ")
    }
    val udfFunc = udf(countriesToString)

    df
      .select(col("name.official").as("Country"), col("borders"))
      .withColumn("NumBorders", size(col("borders")))
      .filter(col("NumBorders") >= 5)
      .withColumn("BorderCountries", udfFunc($"borders"))
      .drop(col("borders"))
      .orderBy(col("NumBorders").desc)
  }

  def getLanguagesInfo(df: DataFrame): DataFrame = {

    df
      .select(col("name.official"), explode(array($"languages.*")))
      .filter(col("col") =!= "NULL")
      .groupBy(col("col").as("Language"))
      .agg(collect_set($"official").as("Countries"), count($"official").as("NumCountries"))
      .orderBy(col("NumCountries").desc)
  }
  getCountriesInfo(countriesDF).show(5, truncate = false)
  getLanguagesInfo(countriesDF).show(10)

}
