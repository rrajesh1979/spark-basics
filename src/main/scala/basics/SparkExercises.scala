package basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object SparkExercises extends App {
  //Logger
  private val logger = LoggerFactory.getLogger(getClass.getName)
  logger.info("Spark Playground")

  //Create a SparkSession
  private val spark = SparkSession.builder()
    .appName("Spark Playground")
    .config("spark.master", "local")
    .getOrCreate()

  /**
   * Exercise:
   * 1) Create a manual DF describing smartphones
   *   - make
   *   - model
   *   - screen dimension
   *   - camera megapixels
   *
   * 2) Read another file from the data/ folder, e.g. movies.json
   *   - print its schema
   *   - count the number of rows, call count()
   */

  private val smartphones = Seq(
    ("Samsung", "Galaxy S10", 6.1, 12),
    ("Apple", "iPhone 11", 6.1, 12),
    ("Google", "Pixel 4", 5.7, 12),
    ("OnePlus", "7T", 6.55, 48),
  )

  private val smartphonesDF = spark.createDataFrame(smartphones)
    .toDF("make", "model", "screen_dimension", "camera_megapixels")

  smartphonesDF.show()

  private val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  println("MoviesDF Schema = " + moviesDF.schema)

  println("MoviesDF Count = " + moviesDF.count())

  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  private val moviesDF2 = moviesDF.select("Title", "Major_Genre")
  moviesDF2.show()

  private val moviesDF3 = moviesDF.select(
    col("Title"),
    col("Major_Genre"),
    col("IMDB_Rating"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross")
  )
  moviesDF3.show()

  private val moviesDF4 = moviesDF.selectExpr(
    "Title",
    "Major_Genre",
    "IMDB_Rating",
    "US_Gross",
    "Worldwide_Gross",
    "US_DVD_Sales",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  moviesDF4.show()

  private val moviesDF5 = moviesDF.select("Title", "Major_Genre", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  moviesDF5.show()
}
