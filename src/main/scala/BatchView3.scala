
case class View3(
	did:String, 
	cid: String,
	deviceName:String,	
	location:String, 
	uniformRoom:String,
	min_time:String,
	max_time:String)

object BatchView3 {

	var view:Dataset[View3] = Seq.empty[View3].toDS


	def construct():Unit = {
		BatchLayer.loadIfNone()
		println("Constructing batch view 3...")

		val toDateTime = udf((ts: Long) => {
			val df = new SimpleDateFormat("yyyy-MM-dd")
			val date = df.format(ts * 1000L)
			date
			
		})

		val toTime = udf((ts: Long) => {
			val df = new SimpleDateFormat("HH:mm")
			val time = df.format(ts * 1000L)
			time
			
		})

		MasterDataset.lectures.as[FlattenedLectureReadings]
			.filter(p => p.lecturers == "Thore Husfeldt")
			.filter(p => p.singularRoom == "4A20")
			.show()

		val v1 = BatchView2
			.view
			.as[View2]
			.join(
					MasterDataset.lectures.as[FlattenedLectureReadings],
					$"singularRoom" === $"uniformRoom" && 
					$"startDate" === $"date" &&
					$"min_time" > $"startTime" &&
					$"max_time" < $"endTime")
			.groupBy("cid","date","startDate", "startTime", "endTime", "singularRoom", "lecturers", "name")
			.agg(count("did"))
			.drop("startDate")
			.withColumnRenamed("name", "courseName")
			.withColumnRenamed("count(did)", "routersUsed")
			//.orderBy(desc("name"))
			//.show()

		v1
			.groupBy("date", "startTime", "endTime", "singularRoom", "lecturers", "courseName")
			.agg(count("cid"))
			.filter(p => p(4) == "Thore Husfeldt")
			.show()

		/*view = MasterDataset
			.devices
			.as[FlattenedReadings]
			.withColumn("date", toDateTime($"ts"))
			.groupBy("date", "did", "cid")
			.agg(toTime(min("ts")),toTime(max("ts")))
			.withColumnRenamed("UDF(min(ts))", "min_time")
			.withColumnRenamed("UDF(max(ts))", "max_time")
			.join(MasterDataset.routers.as[ParsedDeviceReadings], "did")
			.drop("upTime")
			.drop("deviceFunction")
			.drop("deviceMode")
			.as[View3]
			.rdd
			.cache
			.toDS*/

			println("Done constructing view. Row count: " + view.count)
	}
}

BatchView3.construct