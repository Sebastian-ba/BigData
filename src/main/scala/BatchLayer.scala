
object MasterDataset{
	var devices  : Dataset[FlattenedReadings] 		= Seq.empty[FlattenedReadings].toDS
	var routers  : Dataset[ParsedDeviceReadings] 	= Seq.empty[ParsedDeviceReadings].toDS
	var lectures : Dataset[FlattenedLectureReadings] 	= Seq.empty[ParsedLectureReadings].toDS
}

object BatchLayer {

	def start() : Unit = {

		println("Starting Loading of MasterDataset")

		val spark = SparkSession.builder
			.appName("Scala Spark")
			.getOrCreate

		


		//val devices = Dataset
		val files = new java.io.File("../../../data/device/").listFiles.filter(_.getName.endsWith(".json"))
		printList(files)

		//First dataset:
		var devices = spark.read.schema(readingsSchema).json(files(0).toString()).as[Readings]

		//Rest of the datasets
		for (i <- 1 to files.length-1) {
			val newDF = spark.read.schema(readingsSchema).json(files(i).toString()).as[Readings]
			devices = devices.union(newDF)
			println("Number of devices loaded: " + devices.count)
		}

		val flatDeviceDF = flattenDF(devices)
		val deviceDF = fullFlatten(flatDeviceDF)

		val routersDF = spark.read.json("../../../data/routers/meta.json").as[DeviceReadings]
		val parsedRoutersDF = cleanDeviceReadings(routersDF)

		val lectureFiles = new java.io.File("../../../data/lectures/").listFiles.filter(_.getName.endsWith(".json"))
		printList(lectureFiles)
		var lectures = spark.read.json(lectureFiles(0).toString()).as[LectureReadings]		
		for(i <- 1 to lectureFiles.length-1){
			val tmpLectures = spark.read.json(lectureFiles(i).toString())
			if(tmpLectures.count() > 0) {
				val newLecture =  tmpLectures.as[LectureReadings]
				lectures = lectures.union(newLecture)
			}
		}
		val lectureDF = cleanLectureReadings(lectures)


		MasterDataset.devices = deviceDF.as[FlattenedReadings].rdd.cache.toDS
		MasterDataset.routers = parsedRoutersDF.as[ParsedDeviceReadings].rdd.cache.toDS
		MasterDataset.lectures = lectureDF.as[FlattenedLectureReadings].rdd.cache.toDS

		val rowNumber = (MasterDataset.devices.count + MasterDataset.routers.count + MasterDataset.lectures.count)
		println("Master Dataset Loaded. Row count: " + rowNumber)

	}

	def printList(l:Array[java.io.File]) = {
		l.foreach{println}
	}

	def cleanDeviceReadings(df:Dataset[DeviceReadings]): Dataset[ParsedDeviceReadings] = {
		val toUniformRoom = udf((roomStr: String) => {
			//println("->"+roomStr+"<-")
			val roomRegex = """[\w\W]*([\d][\w][\d]{2}[\w]?)""".r
			roomStr match {
				case roomRegex(room) => s"$room"
				case _ => ("") // No room matches, fx: 'change_me'
			}
		})
		val dfRoomConverted = df.withColumn("uniformRoom", toUniformRoom($"location"))
		return dfRoomConverted.asInstanceOf[Dataset[ParsedDeviceReadings]]
	}

	def cleanLectureReadings(df:Dataset[LectureReadings]) : Dataset[FlattenedLectureReadings] = {
		val concatToTimestamp = udf((first: String, second: String) => {
			val tmp = first + " " + second
			val sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm")
			val dt = sdf.parse(tmp)
			(dt.getTime() / 1000)
		})

		val toUniformRoomList = udf((roomStr: String) => {
			val roomRegex = """([\d][\w][\d]{2}[\w]?(?:[-\/](?:[\d]+))?)""".r
			val roomSplitRegex = """([\d][\w])([\d]{2})(?:[-\/]([\d]+))""".r
			var results: Array[String] = Array()
			for (m <- roomRegex.findAllIn(roomStr)) m match {
				case roomSplitRegex(location, room1, room2) => 
					results = (results :+(location+room1)) :+ (location+room2)
				case _ => results = results :+ (m)
			}
			(results)
		})

		val dfStartTimestampConverted = df.withColumn("startTimestamp", concatToTimestamp($"startDate",$"startTime"))
		val dfEndTimestampConverted = dfStartTimestampConverted.withColumn("endTimestamp", concatToTimestamp($"endDate",$"endTime"))
		val dfRoomParsed = dfEndTimestampConverted.withColumn("roomList", toUniformRoomList($"room"))

		return flattenLectureReadings(dfRoomParsed.as[ParsedLectureReadings])
	}

	def flattenLectureReadings(df:Dataset[ParsedLectureReadings]) : Dataset[FlattenedLectureReadings] = {
		df.flatMap(row => {
			println(row.roomList.size)
			val seq = for(i <- 0 until row.roomList.size) yield {
				FlattenedLectureReadings(row.name, row.startDate, row.endDate, row.startTime, row.endTime, row.room, row.lecturers, row.programme, row.startTimestamp, row.endTimestamp, row.roomList, row.roomList(i))
			}
			seq.toSeq
		})
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
