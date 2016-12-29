import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hzcominfo67 on 2016/12/29.
 */
object ElasticSparkSql {
  def main(args: Array[String]) {

    System.setProperty("java.io.tmpdir","D:/albatis/spark-warehouse")

    var session =  SparkSession
      .builder()
    .appName("elasticTest")
    .master("local")
    .config("es.nodes","10.118.159.45")
    .config("es.port","39200")
    .getOrCreate()

   val df = session.read.format("org.elasticsearch.spark.sql").load("scattered_data/ALARM");
    df.show();
  }
}
