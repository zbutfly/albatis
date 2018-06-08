package com.hzcominfo.dataggr.spark.integrate.mongo.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.io.SparkConnection;
import com.hzcominfo.dataggr.spark.util.FuncUtil;

import net.butfly.albacore.io.URISpec;

public class AppTest {
	public static void main(String[] args) {
		URISpec uri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb.PH_ZHK_CZRK");
		SparkConnection client = new SparkConnection("mongodb-apptest", uri);

		Dataset<Row> dataset = client.input(uri).read();
		System.out.println(FuncUtil.rowMap(dataset.first()));
		client.close();
	}
}
