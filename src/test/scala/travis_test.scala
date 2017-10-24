import org.scalatest._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class Travis extends FunSpec with Matchers {

  it("spark") {
    val app = "Scala Spark"
    val conf = new SparkConf().setAppName(app)
    val sc = SparkContext.getOrCreate(conf)

    // val df = spark.read.json("http://130.226.142.195/bigdata/project2/meta.json")

    var spark = SparkSession.builder.appName("myapp").master("local").getOrCreate

    spark should not be null
  }
}
