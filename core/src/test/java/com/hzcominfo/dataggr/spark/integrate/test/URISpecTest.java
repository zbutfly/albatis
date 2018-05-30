package com.hzcominfo.dataggr.spark.integrate.test;

import net.butfly.albacore.io.URISpec;

public class URISpecTest {

	public static void main(String[] args) {
		URISpec u = new URISpec("mongodb://user:pwd@localhost:80/db.tbl");
		System.out.println(u.getFile());
		String file = u.getFile();
		String uri = u.toString();
		if (file.contains(".")) {
			String[] split = file.split("\\.");
			System.err.println("db: " + split[0]);
			System.err.println("tbl: " + split[1]);
			System.out.println(uri.replace("." + split[1], ""));
		}
	}
}
