import org.apache.spark.sql.Dataset



/*{
val devices = _devices
val routers = _routers
val lectures = _lectures

}*/



//val masterDataset:MasterDataset

object BatchLayer {
	case class MasterDataset(var devices:Dataset[FlattenedReadings], var routers:Dataset[DeviceReadings], var lectures:Dataset[ParsedLectureReadings])
	var masterDataset: MasterDataset = MasterDataset(Seq.empty[FlattenedReadings].toDS, Seq.empty[DeviceReadings].toDS, Seq.empty[ParsedLectureReadings].toDS)

	def start() : Unit = {

		println("Starting Batch Layer")

		val spark = SparkSession.builder
			.appName("Scala Spark")
			.getOrCreate
		val conf = new SparkConf().setAppName("BatchLayer")
    	val sc = SparkContext.getOrCreate(conf)


		//val devices = Dataset
		val files = new java.io.File("../../../data/device/").listFiles.filter(_.getName.endsWith(".json"))
		printList(files)

		//First dataset:
		val devices = spark.read.schema(readingsSchema).json(files(0).toString()).as[Readings]

		//Rest of the datasets
		for (i <- 1 to files.length-1) {
			val newDF = spark.read.schema(readingsSchema).json(files(i).toString()).as[Readings]
			devices.union(newDF)
		}

		val flatDeviceDF = flattenDF(devices)
		val deviceDF = fullFlatten(flatDeviceDF)

		val routersDF = spark.read.json("../../../data/routers/meta.json").as[DeviceReadings]
		val parsedRoutersDF = toUniformRoom(routersDF)

		val lectureFiles = new java.io.File("../../../data/lectures/").listFiles.filter(_.getName.endsWith(".json"))
		printList(lectureFiles)
		val lectures = spark.read.json(lectureFiles(0).toString()).as[LectureReadings]

		for(i <- 1 to lectureFiles.length-1){
			val newLecture =  spark.read.json(lectureFiles(i).toString()).as[LectureReadings]
			lectures.union(newLecture)
		}
		val lectureDF = toUnixTimestamp(lectures)

		masterDataset = MasterDataset(deviceDF, parsedRoutersDF, lectureDF)
		println("Master Dataset Loaded")
	}

	def printList(l:Array[java.io.File]) = {
		l.foreach{println}
	}

	def toUniformRoom(df:Dataset[DeviceReadings]): Dataset[ParsedDeviceReadings] = {
		val appendRoom = udf((roomStr: String) => {
			val roomRegex = """[\d][\w][\d]{2}[\w]?""".r
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
