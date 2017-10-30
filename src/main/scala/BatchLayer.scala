import schemas
import batchView1

object batchView1 {
	def construct(deviceDF:Dataset[FlattenedReadings], routersDF:Dataset[DeviceReadings], lectureDF:Dataset[ParsedLectureReadings]):Unit = {
		println("Constructing batch view 1 using parameters...")
		deviceDF.show()
		routersDF.show()
		lectureDF.show()
	}
}

object BatchLayer {

	def start() : Unit = {

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
		println("Devices:")
		devices.show()

		//Rest of the datasets
		for (i <- 1 to files.length-1) {
			val newDF = spark.read.schema(readingsSchema).json(files(i).toString()).as[Readings]
			println(files(i))
			newDF.show()
			devices.union(newDF).collect
		}

		val flatDeviceDF = flattenDF(devices)
		val deviceDF = fullFlatten(flatDeviceDF)

		val routersDF = spark.read.json("../../../data/routers/meta.json").as[DeviceReadings]

		val rawDF = spark.read.json("../../../data/lectures/rooms-2017-10-02.json").as[LectureReadings]
		val lectureDF = toUnixTimestamp(rawDF)

		//CLEANING STEP
		//dataCleaning()
		batchView1.construct(deviceDF, 
			                 routersDF, 
			                 lectureDF)

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
