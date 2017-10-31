
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import spark.implicits._

import java.text.SimpleDateFormat
import java.sql.Date
import java.sql.Timestamp
import org.apache.spark.rdd.RDD


import org.apache.spark.{SparkConf, SparkContext}


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
case class ParsedDeviceReadings (devicename:String, upTime:String, deviceFunction:String, deviceMode:String, did:String, location:String, uniformRoom:String)


case class LectureReadings (name:String, startDate:String, endDate:String, startTime:String, endTime:String, room:String, lecturers:String, programme:String)

case class ParsedLectureReadings (name:String, startDate:String, endDate:String, startTime:String, endTime:String, room:String, lecturers:String, programme:String, startTimestamp:Long, endTimestamp:Long, roomList:String)
