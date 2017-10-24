import org.scalatest._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import collection.mutable.Stack
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

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
        it("Load datafile Spark") {
            val spark = SparkSession.builder.
    					appName("MyApp").
    					master("local").
    					getOrCreate

            val df = spark.read.json("http://130.226.142.195/bigdata/project2/meta.json")
            spark should not be null
            df should not be null
        }
    }
}
