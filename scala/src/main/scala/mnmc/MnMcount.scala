// scalastyle:off println

/*  package main.scala.mnmc 

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Usage: MnMcount <mnm_file_dataset>
  */
object MnMcount {
  def main(args: Array[String]):Unit =  {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .getOrCreate()

    var match_df = spark.read.option("multiline", true).option("inferSchema",true).json("/home/ubuntu/Downloads/competition_match/*.json")
match_df.printSchema
match_df = match_df.drop("resultSet").drop("filters").drop("competition")
match_df.printSchema
match_df.count
match_df = match_df.withColumn("matche", explode(col("matches")))
match_df.count
match_df.printSchema
match_df = match_df.drop("matches")
match_df.printSchema
match_df.count
match_df = match_df.withColumn("id", col("matche").getItem("id"))
match_df.printSchema
match_df.show
match_df = match_df.withColumn("status", col("matche").getItem("status"))
match_df = match_df.withColumn("stage", col("matche").getItem("stage"))
match_df = match_df.withColumn("matchday", col("matche").getItem("matchday"))
match_df.printSchema
match_df.show
match_df = match_df.withColumn("lastUpdated", col("matche").getItem("lastUpdated"))
match_df.printSchema
match_df.show
match_df = match_df.withColumn("homeTeamid", col("matche").getItem("homeTeam").getItem("id"))
match_df.printSchema
match_df.show
match_df = match_df.withColumn("awayTeamid", col("matche").getItem("awayTeam").getItem("id"))
match_df = match_df.withColumn("scorewinner", col("matche").getItem("score").getItem("winner"))
match_df = match_df.withColumn("scoreduration", col("matche").getItem("score").getItem("duration"))
match_df = match_df.withColumn("scorehalfTime", col("matche").getItem("score").getItem("halfTime"))
match_df = match_df.withColumn("scorefullTime", col("matche").getItem("score").getItem("fullTime"))
match_df.printSchema
match_df.show
match_df.count
match_df = match_df.drop("matche")
match_df.printSchema
match_df.write.parquet("/home/ubuntu/Downloads/match2")
  }
} */ 
// scalastyle:on println**/

// scalastyle:off println

package main.scala.mnmc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDate
/**
  * Usage: MnMcount <mnm_file_dataset>
  */
object MnMcount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MnMCount")
      .getOrCreate()

   /* if (args.length < 1) {
      println("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }*/

    //val mnmFile = args(0)

    // Read the M&M file into a Spark DataFrame
   /* val mnmDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Display the M&M DataFrame
    mnmDF.show(5, false)*/

    //chemin hdfs 
    val currentDate = LocalDate.now()
    val paht = s"hdfs://localhost:9000/user/Datalake2/raw/meteo/"+currentDate+".json"

    // Read the JSON file into a DataFrame
   val datajour = spark.read
      .option("multiLine", "true")
      .option("mode", "DROPMALFORMED")
      .option("header", "true")
      .option("inferSchema", "true")
      .json(paht)

    // Display the schema of the DataFrame
  datajour.printSchema()

    //tout les dates
    val dateDF = datajour
      .select(explode(col("list")).as("list_element"))
      .select("list_element.dt_txt")
    dateDF.show(40)
    
    //val capitales = paysdf.select(col("capital"), element_at(col("capital"), 1).as("capital"))
    //capitales.show(40)


    //val continents = paysdf.select(col("continents"), element_at(col("continents"), 1).as("continent"))
    //continents.show(40)

     //temperature minimum temperature
    val tempDF = datajour
      .select(explode(col("list")).as("list_element"))
      .select("list_element.main.temp_min")

    // Convertir temperature  Kelvin enCelsius
    val temperature = datajour
      .select(explode(col("list")).as("list_element"))
      .select(
        expr("CAST(list_element.main.temp_min AS DOUBLE) - 273.15")
          .as("celsius")
      )
    temperature.show(40)

    // Add an index column for the join
    val dateDFIndex = dateDF.withColumn("index", monotonically_increasing_id())
    val temperatureIndex =
      temperature.withColumn("index", monotonically_increasing_id())

    // Jointure des deux table
    val temp_dateJoin =
      dateDFIndex.join(temperatureIndex, Seq("index"), "inner").drop("index")

    // le jour le plus froid
    val dateFroid =
      temp_dateJoin.orderBy(asc("celsius")).select("dt_txt", "celsius")
    dateFroid.show(40)

    // stockage
    
   val cheminpath =
     // "/home/ubuntu/Downloads/mnmcount/scala/data"
     "hdfs://localhost:9000/user/Datalake2/usage/"+currentDate+".json"
    val StorageDatefroid3 = s"$cheminpath/datefroid3"
    dateFroid.write.mode("overwrite").csv(StorageDatefroid3)


    val parquet = s"$cheminpath/parquet2"
    dateFroid.write.mode("overwrite").parquet(parquet)

    // Other commented-out code...
  }
}

