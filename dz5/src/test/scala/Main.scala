//import com.github.mrpowers.spark.fast.tests.DataFrameComparer
//import org.apache.logging.log4j.scala.Logging
import Functions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

object Main extends AnyFlatSpec with Logging with BeforeAndAfter with DataFrameComparer
{

  val spark: SparkSession = SparkSession.builder()
    .appName("SparkMainApp")
    .master("local")
    .getOrCreate

  var testDF: DataFrame = _
  var countriesDF: DataFrame = _
  var languagesDF: DataFrame = _

  val necessaryColumns: Seq[String] = Seq("_id", "altSpellings", "area", "borders", "callingCode", "capital",
    "cca2","cca3", "ccn3", "cioc", "currency", "demonym", "landlocked", "languages", "latlng", "name", "region",
    "subregion", "tld", "translations")

  before {
    testDF = spark
      .read
      .format("json")
      .option("mode", "FAILFAST")
      .load("src/test/resources/countries.json")

    countriesDF = getCountriesInfo(testDF)
    languagesDF = getLanguagesInfo(testDF)
  }

  "This" should "print testDf schema and show" in {
    testDF.printSchema
    testDF.show
  }

  "testDF" should "contain all necessary columns" in {
    print(testDF)
    assert(necessaryColumns.forall(x => testDF.columns.contains(x)))
  }

  "getCountriesInfo" should "show countries with region 'Africa'" in {
    assert(countriesDF.select("region").filter(col("region") === "Africa").count() === 58)
  }

  "getLanguagesInfo" should "have top 3 languages the same as in etalon DF" in {
    import spark.implicits._
    val etalonTopLanguageDF = Seq("English", "French", "Arabic").toDF("Language")
    assertSmallDataFrameEquality(languagesDF.select(col("Language")).limit(3), etalonTopLanguageDF)
  }

  "getLanguagesInfo" should "have etalon struture schema" in {
    val arrayStructureSchema = new StructType()
      .add("Country",StringType)
      .add("NumBorders", LongType, nullable = false)
      .add("BorderCountries", ArrayType(StringType, containsNull = false), nullable = false)
    assert(countriesDF.schema === arrayStructureSchema)
  }
}
