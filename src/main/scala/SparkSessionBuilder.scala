import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  
  def SparkSessionBuilder(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("data_exploitation")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.sql.ansi.enabled", "false")
      .getOrCreate()
  
    return spark
    }
}
