import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.sql.Date

object PegiRatings {

	type Pegi = (String,Integer,String,String,Date)

	type Ign = (Integer,String,String,String,String,Double,String,String,Integer,Integer,Integer)

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

	def pegi18 (pegiDF:Dataset[Pegi]): Dataset[Pegi] = {
		pegiDF.filter(pegi => pegi._2 == 18)
	}

	def pegiCount18 (pegiDF:Dataset[Pegi]): Long = {
		pegi18(pegiDF).count
	}

	def pegi18forPS2toPS4 (pegiDF:Dataset[Pegi]): (String,Int) = {
		val ps2 = "Playstation 2"
		val ps3 = "Playstation 3"
		val ps4 = "PlayStation 4"
		// Solution 1
		//pegi18(pegiDF)
		//	.map(pegi => pegi._3)
		//	.filter(v => v == ps2 || v == ps3 || v == ps4)
		//	.count

		// Solution 2
		pegi18(pegiDF)
			.groupByKey(pegi => pegi._3)
			.mapGroups{
				case (k:String,v:Iterator[Pegi]) =>
					if(k == ps2 || k == ps3 || k == ps4)
						("PS",v.length)
					else
						("Other",v.length)
			}
			.filter((kv:(String,Int)) => kv._1 == "PS")
			.reduce((v1:(String,Int), v2:(String,Int)) => ("PS",v1._2 + v2._2))
	}

	def pegi18Newest (pegiDF:Dataset[Pegi]): Dataset[Pegi] = {
		pegi18(pegiDF).orderBy(desc("releaseDate"))
	}

	def highestGenreCountSince2010 (pegiDF:Dataset[Pegi]): List[(String,Int)] = {
		val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		val date = LocalDate.parse("2010-01-01", formatter);

		val from2010 = pegiDF
						.map(pegi => (pegi._5, pegi._4))
						.filter(dsGenre => dsGenre._1 != null)
						.filter(dsGenre => LocalDate.parse(dsGenre._1.toString).getYear > date.getYear)

		from2010.groupByKey(ds => ds._2)
				.mapGroups((k,v) => (k,v.length))
				.orderBy(desc("_2"))
				.take(3)
				.toList
	}

	def highestRatedIGNfromPEGI (pegiDF:Dataset[Pegi]) (ignDF:Dataset[Ign]) = {
		// Note ign platforms use PlayStation 3 and not Playstation 3 as Pegi ratings
		// Rename the platforms to lowercase
		val pegiDFlc = pegiDF.withColumn("platform", lower(col("platform")))

		val ignDFlc = ignDF.withColumn("ignPlatform", lower(col("ignPlatform")))

		pegiDFlc
			.join(ignDFlc, pegiDFlc("pegiTitle") === ignDFlc("ignTitle") && pegiDFlc("platform") === ignDFlc("ignPlatform"))
			.select("ignTitle", "ignPlatform", "ignRating", "pegiRating", "genre")
			.orderBy(desc("ignRating"))
	}

	def main = {
		val pegiDF = pegiDFLoader("data/pegi_ratings.csv")

		println("No. of 18+ games: " + pegiCount18(pegiDF))
		println("No. of PS2-PS4 games with 18+ rating: " + pegi18forPS2toPS4(pegiDF)._2)
		println("Newest 18+ game: " + pegi18Newest(pegiDF).first)
		println("Since 2010 the top 3 genres have been: " + highestGenreCountSince2010(pegiDF))

		val ignDF = ignDFLoader("data/ign_reviews.csv")

		(highestRatedIGNfromPEGI (pegiDF) (ignDF)).show
	}

}
