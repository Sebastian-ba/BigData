import schemas
import batchView1

object scalaSpark {

	def start() : Unit = {
		val spark = SparkSession.builder.
						appName("Scala Spark").
						getOrCreate

		val rawDeviceDF = spark.read.schema(readingsSchema).json("../data/2-10-2017.json").as[Readings]
		val flatDeviceDF = flattenDF(rawDeviceDF)
		val deviceDF = fullFlatten(flatDeviceDF)

		val routersDF = spark.read.json("../data/meta.json").as[DeviceReadings]
		val parsedRoutersDF = toUniformRoom(routersDF)

		val rawDF = spark.read.json("../data/rooms-2017-10-02.json").as[LectureReadings]
		val lectureDF = toUnixTimestamp(rawDF)

		//CLEANING STEP
		dataCleaning()

		//val batchView1 = new batchView1(deviceDF, routersDF, lectureDF)
		batchView1.construct(deviceDF,
			                 parsedRoutersDF,
			                 lectureDF)

	}

	def dataCleaning() = {

	}
	def toUniformRoom(df:Dataset[DeviceReadings]): Dataset[ParsedDeviceReadings] = {
		val appendRoom = udf((roomStr: String) => {
			val roomRegex = "[\d][\w][\d]{2}[\w]?".r
			roomStr match {
				case (room) => s"$room"
			}
		})
		val dfRoomConverted = df.withColumn("uniformRoom", appendRoom($"location"))
		return dfRoomConverted.asInstanceOf[Dataset[ParsedDeviceReadings]]
	}

	def toUnixTimestamp(df:Dataset[LectureReadings]) : Dataset[ParsedLectureReadings] = {
		val concatToTimestamp = udf((first: String, second: String) => {
			val tmp = first + " " + second
			val sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm")
			val dt = sdf.parse(tmp)
			(dt.getTime() / 1000)
		})

		val appendRoomList = udf((roomStr: String) => {
			val roomRegex = "((?:[\d][\w][\d]{2}[\w]?(?:[-\/](?:[\d]+))?))[,\s]*".r
			val roomSplitRegex = "[\d][\w][\d]{2}(?:[-\/]([\d]+))".r
			val result = ""
			for (m <- roomRegex.findAllIn(roomStr)) {
				roomStr match {
					case (room) => s"$room"
				}
			}
		})

		val dfStartTimestampConverted = df.withColumn("startTimestamp", concatToTimestamp($"startDate",$"startTime"))
		val dfEndTimestampConverted = dfStartTimestampConverted.withColumn("endTimestamp", concatToTimestamp($"endDate",$"endTime"))
		val dfRoomParsed = dfEndTimestampConverted.withColumn("roomList", appendRoomList($"room"))

		return dfRoomParsed.asInstanceOf[Dataset[ParsedLectureReadings]]
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
