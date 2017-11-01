
case class View2(
	did:String, 
	cid: String,
	date: String,
	deviceName:String,	
	location:String, 
	uniformRoom:String,
	min_time:String,
	max_time:String)

object BatchView2 {

	var view:Dataset[View2] = Seq.empty[View2].toDS


	def construct():Unit = {
		BatchLayer.loadIfNone()
		println("Constructing batch view 2...")

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

		view = MasterDataset
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
			.as[View2]
			.rdd
			.cache
			.toDS

			println("Done constructing view. Row count: " + view.count)
	}
}

BatchView2.construct