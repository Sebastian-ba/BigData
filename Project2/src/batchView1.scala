
object batchView1 {
	def construct(deviceDF:Dataset[FlattenedReadings], routersDF:Dataset[DeviceReadings], lectureDF:Dataset[ParsedLectureReadings]):Unit = {
		println("Constructing batch view 1 using parameters...")
		deviceDF.show()
		routersDF.show()
		lectureDF.show()
	}
}