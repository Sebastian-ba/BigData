
object BatchView1 {

	var view:Dataset[FlattenedReadings] = Seq.empty[FlattenedReadings].toDS


	def construct():Unit = {
		println("Constructing batch view 1...")
		view = BatchLayer.masterDataset.devices
		view.show()
	}
}