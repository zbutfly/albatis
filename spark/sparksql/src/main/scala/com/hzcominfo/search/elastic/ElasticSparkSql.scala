package com.hzcominfo.search.elastic

import org.apache.spark.sql.SparkSession

object ElasticSparkSql {
  def main(args: Array[String]) {
    var session = SparkSession
      .builder()
      .appName("elasticTest")
      .master("local")
      .config("es.nodes", "10.118.159.45")
      .config("es.port", "39200")
      .config("es.read.field.as.array.include","true")
      //.config("spark.sql.warehouse.dir", "file:///D:/albatis/spark-warehouse")
      .getOrCreate()

    val df = session.read.format("org.elasticsearch.spark.sql")
      .load("net_police_201701/HZGA_WA_SOURCE_FJ_1001")
    df.show()
  }
}
