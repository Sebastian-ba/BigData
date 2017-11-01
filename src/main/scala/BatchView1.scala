
// Get a dataset that describe the connection strength for each router at each day.

// Case class for the output table structure.
// Did | location | deviceFunction | deviceMode | uniformRoom | avg(rssi) | avg(snRatio) | day | ts
case class View1(did:String, 
		location:String, 
		deviceFunction:String, 
		deviceMode:String, 
		uniformRoom:String,
		avgRssi:Double,
		avgSnRatio:Double,
		day:String)

object BatchView1 {

	var view:Dataset[View1] = Seq.empty[View1].toDS


	def construct():Unit = {
		println("Constructing batch view 1...")
		
		val devices = BatchLayer.masterDataset.devices
		val routers = BatchLayer.masterDataset.routers
		devices.show
		routers.show

	}


}