import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SparkSessionTest extends AnyFunSuite with BeforeAndAfterAll {

  // spark devient un lazy val => stable pour les imports
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("test_spark_session")
    .master("local[*]")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("SparkSession should be created successfully") {
    assert(spark != null)
    assert(spark.sparkContext.appName == "test_spark_session")
    assert(!spark.sparkContext.isStopped)
  }

  test("SparkSession should be able to create a simple DataFrame") {
    import spark.implicits._   // maintenant autorisé, car spark est un val

    val data = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35))
    val df = data.toDF("name", "age")

    assert(df.count() == 3)
    assert(df.columns.length == 2)
    assert(df.columns.contains("name"))
    assert(df.columns.contains("age"))
  }
}
