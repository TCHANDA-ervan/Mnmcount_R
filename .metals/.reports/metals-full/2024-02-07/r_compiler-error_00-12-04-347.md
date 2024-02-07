file://<WORKSPACE>/scala/src/main/scala/mnmc/MnMcount.scala
### java.lang.AssertionError: assertion failed: denotation object language invalid in run 3. ValidFor: Period(1..2, run = 4)

occurred in the presentation compiler.

action parameters:
uri: file://<WORKSPACE>/scala/src/main/scala/mnmc/MnMcount.scala
text:
```scala
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

    var match_df = spark.read.option("multiline", true).option("inferSchema",true).json("<HOME>/Downloads/competition_match/*.json")
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
match_df.write.parquet("<HOME>/Downloads/match2")
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
  paysdf.printSchema()

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
     // "<HOME>/Downloads/mnmcount/scala/data"
     "hdfs://localhost:9000/user/Datalake2/usage/"+currentDate+".json"
    val StorageDatefroid3 = s"$cheminpath/datefroid3"
    dateFroid.write.mode("overwrite").csv(StorageDatefroid3)


    val parquet = s"$cheminpath/parquet2"
    dateFroid.write.mode("overwrite").parquet(parquet)

    // Other commented-out code...
  }
}


```



#### Error stacktrace:

```
scala.runtime.Scala3RunTime$.assertFailed(Scala3RunTime.scala:8)
	dotty.tools.dotc.core.Denotations$SingleDenotation.updateValidity(Denotations.scala:717)
	dotty.tools.dotc.core.Denotations$SingleDenotation.bringForward(Denotations.scala:742)
	dotty.tools.dotc.core.Denotations$SingleDenotation.toNewRun$1(Denotations.scala:799)
	dotty.tools.dotc.core.Denotations$SingleDenotation.current(Denotations.scala:870)
	dotty.tools.dotc.core.Symbols$Symbol.recomputeDenot(Symbols.scala:120)
	dotty.tools.dotc.core.Symbols$Symbol.computeDenot(Symbols.scala:114)
	dotty.tools.dotc.core.Symbols$Symbol.denot(Symbols.scala:107)
	dotty.tools.dotc.core.Symbols$.toDenot(Symbols.scala:494)
	dotty.tools.dotc.typer.Checking.checkLegalImportPath(Checking.scala:938)
	dotty.tools.dotc.typer.Checking.checkLegalImportPath$(Checking.scala:809)
	dotty.tools.dotc.typer.Typer.checkLegalImportPath(Typer.scala:116)
	dotty.tools.dotc.typer.Typer.typedImport(Typer.scala:2789)
	dotty.tools.dotc.typer.Typer.typedUnnamed$1(Typer.scala:3060)
	dotty.tools.dotc.typer.Typer.typedUnadapted(Typer.scala:3112)
	dotty.tools.dotc.typer.Typer.typed(Typer.scala:3184)
	dotty.tools.dotc.typer.Typer.typed(Typer.scala:3188)
	dotty.tools.dotc.typer.Typer.traverse$1(Typer.scala:3200)
	dotty.tools.dotc.typer.Typer.typedStats(Typer.scala:3256)
	dotty.tools.dotc.typer.Typer.typedPackageDef(Typer.scala:2812)
	dotty.tools.dotc.typer.Typer.typedUnnamed$1(Typer.scala:3081)
	dotty.tools.dotc.typer.Typer.typedUnadapted(Typer.scala:3112)
	dotty.tools.dotc.typer.Typer.typed(Typer.scala:3184)
	dotty.tools.dotc.typer.Typer.typed(Typer.scala:3188)
	dotty.tools.dotc.typer.Typer.typedExpr(Typer.scala:3300)
	dotty.tools.dotc.typer.TyperPhase.typeCheck$$anonfun$1(TyperPhase.scala:44)
	dotty.tools.dotc.typer.TyperPhase.typeCheck$$anonfun$adapted$1(TyperPhase.scala:54)
	scala.Function0.apply$mcV$sp(Function0.scala:42)
	dotty.tools.dotc.core.Phases$Phase.monitor(Phases.scala:440)
	dotty.tools.dotc.typer.TyperPhase.typeCheck(TyperPhase.scala:54)
	dotty.tools.dotc.typer.TyperPhase.runOn$$anonfun$3(TyperPhase.scala:88)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.immutable.List.foreach(List.scala:333)
	dotty.tools.dotc.typer.TyperPhase.runOn(TyperPhase.scala:88)
	dotty.tools.dotc.Run.runPhases$1$$anonfun$1(Run.scala:246)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:15)
	scala.runtime.function.JProcedure1.apply(JProcedure1.java:10)
	scala.collection.ArrayOps$.foreach$extension(ArrayOps.scala:1321)
	dotty.tools.dotc.Run.runPhases$1(Run.scala:262)
	dotty.tools.dotc.Run.compileUnits$$anonfun$1(Run.scala:270)
	dotty.tools.dotc.Run.compileUnits$$anonfun$adapted$1(Run.scala:279)
	dotty.tools.dotc.util.Stats$.maybeMonitored(Stats.scala:67)
	dotty.tools.dotc.Run.compileUnits(Run.scala:279)
	dotty.tools.dotc.Run.compileSources(Run.scala:194)
	dotty.tools.dotc.interactive.InteractiveDriver.run(InteractiveDriver.scala:165)
	scala.meta.internal.pc.MetalsDriver.run(MetalsDriver.scala:45)
	scala.meta.internal.pc.SemanticdbTextDocumentProvider.textDocument(SemanticdbTextDocumentProvider.scala:33)
	scala.meta.internal.pc.ScalaPresentationCompiler.semanticdbTextDocument$$anonfun$1(ScalaPresentationCompiler.scala:191)
```
#### Short summary: 

java.lang.AssertionError: assertion failed: denotation object language invalid in run 3. ValidFor: Period(1..2, run = 4)