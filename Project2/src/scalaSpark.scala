import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object scalaSpark extends Serializable{

	def printList(args: TraversableOnce[_]): Unit = {
  		args.foreach(println)
	}

	def start() : Unit = {
	    val app = "Scala Spark";
	    val conf = new SparkConf().setAppName(app);
	    val sc = SparkContext.getOrCreate(conf);


	    val df = spark.read.json("3-10-2017.json");

	    df.show();

		df.printSchema();
		//val list = df.takeAsList(2);
		//list.printList;

	    //val dataFile = sc.textFile(args(0))
	    //val output = calculateKeyFigures(dataFile)
	    //output.saveAsTextFile("file:////" + args(1))
	  }
}