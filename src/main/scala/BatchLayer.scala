
object BatchLayer {
	case class MasterDataset(var devices:Dataset[FlattenedReadings], var routers:Dataset[DeviceReadings], var lectures:Dataset[ParsedLectureReadings])
	var masterDataset: MasterDataset = MasterDataset(Seq.empty[FlattenedReadings].toDS, Seq.empty[DeviceReadings].toDS, Seq.empty[ParsedLectureReadings].toDS)

	def start() : Unit = {

		println("Starting Batch Layer")

		val spark = SparkSession.builder
			.appName("Scala Spark")
			.getOrCreate


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


		val lectureFiles = new java.io.File("../../../data/lectures/").listFiles.filter(_.getName.endsWith(".json"))
		printList(lectureFiles)
		val lectures = spark.read.json(lectureFiles(0).toString()).as[LectureReadings]

		for(i <- 1 to lectureFiles.length-1){
			val tmpLectures = spark.read.json(lectureFiles(i).toString())
			if(tmpLectures.count() > 0) {
				val newLecture =  tmpLectures.as[LectureReadings]
				lectures.union(newLecture)
			}
		}
		val lectureDF = toUnixTimestamp(lectures)

		masterDataset = MasterDataset(deviceDF, routersDF, lectureDF)
		println("Master Dataset Loaded")

	}

	def printList(l:Array[java.io.File]) = {
		l.foreach{println}
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
