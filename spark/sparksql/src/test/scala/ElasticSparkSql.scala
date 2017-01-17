import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.{SparkContext, SparkConf}
object ElasticSparkSql {
  def main(args: Array[String]) {
    var session = SparkSession
      .builder()
      .appName("elasticTest")
      .master("local")
      .config("es.nodes", "10.118.159.44")
      .config("es.port", "39210")
     // .config("spark.sql.warehouse.dir", "file:///D:/albatis/spark-warehouse")
      .getOrCreate()

    val df = session.read.format("org.elasticsearch.spark.sql")
      .load("social_information_phgadev/CU_AJDSRXX")
    df.show()
  }
}
