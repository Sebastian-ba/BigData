object BatchView3 {

	var view:Dataset[FlattenedReadings] = Seq.empty[FlattenedReadings].toDS

	def construct():Unit = {
		println("Constructing batch view 3...")
		view = BatchLayer.masterDataset.devices
		view.show()
	}
}