import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date

object LoadFiles {

	type Pegi = (String,Integer,String,String,Date)

	type Ign = (Integer,String,String,String,String,Double,String,String,Integer,Integer,Integer)

	//INSERT YOUR OWN PATH FOR THE DATA FILES
	val pegiPath = "data/pegi_ratings.csv"
	val ignPath = "data/ign_reviews.csv"

	val spark = SparkSession.builder.
					appName("MyApp").
					master("local").
					getOrCreate

	import spark.implicits._
	
	def pegiDFLoader (path:String): Dataset[Pegi] = {
		spark.read
			 .schema(Encoders.product[Pegi].schema)
			 .option("header","true")
			 .csv(path)
			 .withColumnRenamed("_1","pegiTitle")
			 .withColumnRenamed("_2","pegiRating")
			 .withColumnRenamed("_3","platform")
			 .withColumnRenamed("_4","genre")
			 .withColumnRenamed("_5","releaseDate")
			 .as[Pegi]
	}

	def ignDFLoader (path:String): Dataset[Ign] = {
		spark.read
			 .schema(Encoders.product[Ign].schema)
			 .option("header","true")
			 .csv(path)
			 .withColumnRenamed("_1","id")
			 .withColumnRenamed("_2","scorePhrase")
			 .withColumnRenamed("_3","ignTitle")
			 .withColumnRenamed("_4","url")
			 .withColumnRenamed("_5","ignPlatform")
			 .withColumnRenamed("_6","ignRating")
			 .withColumnRenamed("_7","ignGenre")
			 .withColumnRenamed("_8","editors_choice")
			 .withColumnRenamed("_9","year")
			 .withColumnRenamed("_10","month")
			 .withColumnRenamed("_11","day")
			 .as[Ign]
	}

	// Simple counting example done with groupByKey and mapGroups
	def groupingExample : Unit = {
		val pegiDF = pegiDFLoader(pegiPath)

		pegiDF
			.groupByKey(_._4)
			.mapGroups{
				case (key, iter) => (key, iter.length)
			}
			.filter(kV => kV._1 == "Other")
			.show
	}

	def filteringExample : Long = {
		val pegiDF = pegiDFLoader(pegiPath)

		pegiDF.filter(p => p._4 == "Other").count
	}

}