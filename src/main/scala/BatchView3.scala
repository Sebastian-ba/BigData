
case class View3(cid: String,
	date: String, 
	startTime: String,
	endTime: String,
	singularRoom: String,
	lecturers: String,
	courseName: String,
	programme: String,
	routersUsed: Long)

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

		view = BatchView2
			.view
			.as[View2]
			.join(
					MasterDataset.lectures.as[FlattenedLectureReadings].distinct(),
					$"singularRoom" === $"uniformRoom" && 
					$"startDate" === $"date" &&
					$"min_time" > $"startTime" &&
					$"max_time" < $"endTime")
			.groupBy("cid","date","startDate", "startTime", "endTime", "singularRoom", "lecturers", "name", "programme")
			.agg(count("did"))
			.drop("startDate")
			.withColumnRenamed("name", "courseName")
			.withColumnRenamed("count(did)", "routersUsed")
			.as[View3]
			.rdd
			.cache
			.toDS

		println("Done constructing view. Row count: " + view.count)
	}

	def query1: Unit = {
		view.groupBy("date", "startTime", "endTime", "singularRoom", "lecturers", "courseName", "programme")
			.agg(count("cid"))
			.filter(p => p(4) == "Thore Husfeldt")
			.show()
	}
}

BatchView3.construct