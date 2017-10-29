
object batchView1 {
	def construct[T](deviceDF:Dataset[T]):Unit = {
		println("Constructing batch view 1 using parameters...")
		deviceDF.show()
	}
}

/*class BatchView1() {
	def construct[T](lectureDF:Dataset[T]) {
		println("Constructing batch view 1 using parameters...")
		lectureDF.show()
	}
}*/