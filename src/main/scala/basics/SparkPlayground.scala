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
    expr("Weight_in_lbs"),   //Expression
    (col("Weight_in_lbs") /2.2).as("Weight_in_kg"), //Expression with alias) ,
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs")    //Expression
  ).show()

  //Option 3 - select with list of expressions
  carsDF.select(
    expr("Name"),
    expr("Weight_in_lbs as Weight_Pounds"),    //Expression with alias
    expr("Weight_in_lbs / 2.2 as `Weight in Kgs`"), //Expression with alias and spaces
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs")    //Expression with alias
  ).show()

  //Option 4 - select with list of column names and expressions
  carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as `Weight in Kgs`", //Expression with alias and spaces
    "Weight_in_lbs / 2.2 as Weight_in_kgs"    //Expression with alias
  ).show()

  //New DataFrame with a new column, "Weight_in_kgs" and existing column "Weight_in_lbs" renamed to "Weight_in_pounds"
  private val carsDFWithWeightInKgs = carsDFWithSchema.select(
    col("Name"),
    col("Weight_in_lbs").as("Weight_in_pounds"),
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kgs")
  )
  logger.info("carsDFWithWeightInKgs Schema = " + carsDFWithWeightInKgs.schema)
  logger.info("New DataFrame")
  carsDFWithWeightInKgs.show()

  private val carsDFNew = carsDFWithSchema
    .withColumn("Weight_in_kgs", col("Weight_in_lbs") / 2.2)
    .withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
    .drop("Acceleration") //Drop a column

  logger.info("carsDFNew Schema = " + carsDFNew.schema)
  logger.info("New DataFrame")
  carsDFNew.show()

  //Filtering
  private val americanCarsDF =
    carsDFNew
    .filter(
      col("Origin") === "USA"
    )

  //Chaining filters
  private val americanPowerfulCarsDF =
    carsDFNew
    .filter(
      col("Origin") === "USA"
    )
    .filter(
      col("Horsepower") > 150
    )

  //Filtering with expressions
  private val americanPowerfulCarsDF2 =
    carsDFNew
    .filter(
      "Origin = 'USA' and Horsepower > 150"
    )

  //Filtering with logical expressions
  private val americanPowerfulCarsDF3 =
    carsDFNew
      .filter(
        (col("Origin") === "USA") and (col("Horsepower") > 150)
      )




}
