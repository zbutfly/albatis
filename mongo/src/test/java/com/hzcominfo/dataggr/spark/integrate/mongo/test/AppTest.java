package com.hzcominfo.dataggr.spark.integrate.mongo.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.dataggr.spark.integrate.Client;
import com.hzcominfo.dataggr.spark.integrate.util.FuncUtil;

import net.butfly.albacore.io.URISpec;

public class AppTest {
	
	public static void main(String[] args) {
		URISpec uri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb.PH_ZHK_CZRK");
		Client client = new Client("mongodb-apptest", uri);
		Dataset<Row> dataset = client.read();
		FuncUtil.func.accept(dataset.first());
		client.close();
	}
}
