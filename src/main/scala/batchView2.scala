object BatchView2 {

	var view:Dataset[FlattenedReadings] = Seq.empty[FlattenedReadings].toDS

	def construct():Unit = {
		println("Constructing batch view 2...")
		view = BatchLayer.masterDataset.devices
		view.show()
	}
}