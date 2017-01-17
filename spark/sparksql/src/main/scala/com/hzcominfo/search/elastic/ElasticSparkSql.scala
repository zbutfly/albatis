package com.hzcominfo.search.elastic

import org.apache.spark.sql.SparkSession

object ElasticSparkSql {

  def main(args: Array[String]) {
    table()
  }

  def table(): Unit = {
    val session = SparkSession.builder()
      .appName("elasticTest")
      .master("local[10]")
      .config("spark.sql.warehouse.dir", "file:///D:/albatis/spark-warehouse")
      .getOrCreate()
    val op = Map("es.nodes" -> "10.118.159.45", "es.port" -> "39200", "es.scroll.size" -> "90000",
      "es.read.field.as.array.include" -> "allPhones,callingidcards,calledidcards,idcards,receiptAddresss")

    val df = session.read.format("org.elasticsearch.spark.sql").options(op)
      .load("scattered_data_2017/phone_bill").limit(50000)
    df.createOrReplaceTempView("t")

    val df2 = session.read.format("org.elasticsearch.spark.sql").options(op)
      .load("scattered_data/phone_bill_increment").limit(50000)
    df2.createOrReplaceTempView("s")


    val sql = session.sql("select t.callingAttr from t ,s where t.idcards = s.idcards")
    sql.show(270000)

  }
}
