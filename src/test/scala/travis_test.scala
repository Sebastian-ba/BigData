import org.scalatest._
import collection.mutable.Stack
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Readings (did:String, readings:Array[(Array[(String,String,Double,Double,String)],Long)])

class Travis extends FunSpec with Matchers {
    describe("A Set") {
        describe("when empty") {
            it("should have size 0") {
                assert(Set.empty.size == 0)
            }

            it("should produce NoSuchElementException when head is invoked") {
                assertThrows[NoSuchElementException] {
                    Set.empty.head
                }
            }
        }
    }

    describe("Spark") {
        it("Init Spark") {
            val spark = SparkSession.builder.
    					appName("MyApp").
    					master("local").
    					getOrCreate
            spark should not be null
        }
        it("Spark reading") {
            val spark = SparkSession.builder.
						appName("Scala Spark").
						getOrCreate
            import spark.implicits._
            val readingsSchema = StructType(Array(
            				StructField("did",StringType,true),
            				StructField("readings",ArrayType(StructType(Array(
            					StructField("clients",ArrayType(StructType(Array(
            						StructField("cid",StringType,true),
            						StructField("clientOS",StringType,true),
            						StructField("rssi",DoubleType,true),
            						StructField("snRatio",DoubleType,true),
            						StructField("ssid",StringType,true))),true),true),
            					StructField("ts",LongType,true))),true),true)))

            val rawDeviceDF = spark.read.schema(readingsSchema).json("src/test/scala/data/2-10-2017.json").as[Readings]
            rawDeviceDF should not be null
            // rawDeviceDF should equal(Array(
            //     Readings("d7cc92c24be32d5d419af1277289313c", Array(
            //         (Array(), 1506895301)
            //     ))
            // ))
        }
    }
}
