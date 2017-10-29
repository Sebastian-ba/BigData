import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import spark.implicits._

import java.text.SimpleDateFormat
import java.sql.Date
import java.sql.Timestamp


case class Readings (did:String, readings:Array[(Array[(String,String,Double,Double,String)],Long)])
case class ExplodedReadings (did: String, readings:(Array[(String,String,Double,Double,String)],Long))
case class FlattenedReadingsInput (did:String, cid:Array[String], clientOS:Array[String], rssi:Array[Double], snRatio:Array[Double], ssid:Array[String], ts:Long)
case class FlattenedReadings (did:String, cid:String, clientOS:String, rssi:Double, snRatio:Double, ssid:String, ts:Long)
//Schema from Omar Shahbaz Khan
val readingsSchema = StructType(Array(
				StructField("did",StringType,true), 
				StructField("readings",ArrayType(StructType(Array(
					StructField("clients",ArrayType(StructType(Array(
						StructField("cid",StringType,true), 
						StructField("clientOS",StringType,true), 
						StructField("rssi",DoubleType,true), 
						StructField("snRatio",DoubleType,true), 
						StructField("ssid",StringType,true))),true),true), 
					StructField("ts",LongType,true))),true),true)))


case class DeviceReadings (devicename:String, upTime:String, deviceFunction:String, deviceMode:String, did:String, location:String)


case class LectureReadings (name:String, startDate:String, endDate:String, startTime:String, endTime:String, room:String, lecturers:String, programme:String)

case class ParsedLectureReadings (name:String, startDate:String, endDate:String, startTime:String, endTime:String, room:String, lecturers:String, programme:String, startTimestamp:Long, endTimestamp:Long)

object scalaSpark {

	def start() : Unit = {
		val spark = SparkSession.builder.
						appName("Scala Spark").
						getOrCreate

		val rawDeviceDF = spark.read.schema(readingsSchema).json("../data/2-10-2017.json").as[Readings]
		val flatDeviceDF = flattenDF(rawDeviceDF)
		val deviceDF = fullFlatten(flatDeviceDF)

		val routersDF = spark.read.json("../data/meta.json").as[DeviceReadings]

		val rawDF = spark.read.json("../data/rooms-2017-10-02.json").as[LectureReadings]

		val lectureDF = toUnixTimestamp(rawDF)

		lectureDF.show()
	}

	def toUnixTimestamp(df:Dataset[LectureReadings]) : Dataset[ParsedLectureReadings] = {
		val concatToTimestamp = udf((first: String, second: String) => {
			val tmp = first + " " + second
			val sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm")
			val dt = sdf.parse(tmp)
			(dt.getTime() / 1000)
		})

		val dfStartTimestampConverted = df.withColumn("startTimestamp", concatToTimestamp($"startDate",$"startTime"))
		val dfEndTimestampConverted = dfStartTimestampConverted.withColumn("endTimestamp", concatToTimestamp($"endDate",$"endTime"))

		return dfEndTimestampConverted.asInstanceOf[Dataset[ParsedLectureReadings]]
	}

	def fullFlatten(df:Dataset[FlattenedReadingsInput]) : Dataset[FlattenedReadings] = {
		df.flatMap(row => {
	        val seq = for( i <- 0 until row.cid.size) yield { 
	        	FlattenedReadings(row.did, row.cid(i), row.clientOS(i), row.rssi(i), row.snRatio(i), row.ssid(i), row.ts)
	        }
	        seq.toSeq			
		})
    }

	def flattenDF (df:Dataset[Readings]): Dataset[FlattenedReadingsInput] = {
		val expDF = df.withColumn("readings", explode(col("readings"))).as[ExplodedReadings]
		expDF
			.select($"did",$"readings.clients.cid",$"readings.clients.clientOS",$"readings.clients.rssi",$"readings.clients.snRatio",$"readings.clients.ssid",$"readings.ts")
			.drop("readings")
			.as[FlattenedReadingsInput]
	}
}