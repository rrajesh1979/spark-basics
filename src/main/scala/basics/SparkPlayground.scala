package basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

object SparkPlayground extends App {
  //Logger
  private val logger = LoggerFactory.getLogger(getClass.getName)
  logger.info("Spark Playground")

  //Create a SparkSession
  private val spark = SparkSession.builder()
    .appName("Spark Playground")
    .config("spark.master", "local")
    .getOrCreate()

  //Read cars.json into a DataFrame
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //Print the schema
  println("CarsDF Schema = " + carsDF.schema)
  carsDF.printSchema()

  //Show the DataFrame
  carsDF.show()

  private val carsDFSchema = StructType(Array(
    StructField("Acceleration",DoubleType,true),
    StructField("Cylinders",LongType,true),
    StructField("Displacement",DoubleType,true),
    StructField("Horsepower",LongType,true),
    StructField("Miles_per_Gallon",DoubleType,true),
    StructField("Name",StringType,true),
    StructField("Origin",StringType,true),
    StructField("Weight_in_lbs",LongType,true),
    StructField("Year",StringType,true)
  ))

  private val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  println("CarsDFWithSchema Schema = " + carsDFWithSchema.schema)

  //Show the DataFrame
  carsDFWithSchema.show()

  //Various Select methods
  //Option 1 - select with list of column names
  carsDF.select("Name").show()

  //Option 2 - select with list of column objects
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Name"),           //Same as above
    column("Year"),        //Same as above
    'Origin,                        //Scala symbol, auto-converted to column
    Symbol("Origin"),               //Symbol literals are deprecated in Scala 2.13. Use Symbol("Origin") instead
    $"Cylinders",                   //String interpolation
    expr("Weight_in_lbs")     //Expression
  ).show()

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


}
