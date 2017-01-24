import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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
class ElasticSparkSql{
  def test(): Unit = {
    val spark = new SparkContext()
    val data = Array(1, 2, 3, 4, 5, 6)
    val distData = spark.parallelize(data)
    distData.map(a=>a+1)
    distData.collect()
    val f = spark.textFile("")
   // f.map()



  }
  def test1(a: Int, v: Int = 1): Unit = {

  }
}
