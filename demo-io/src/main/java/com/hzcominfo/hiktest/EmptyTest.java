package com.hzcominfo.hiktest;

import java.io.IOException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.Connection;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

public class EmptyTest {
	public static String uri = "kafka://demo01:2281,demo02:2281,demo03:2281/?df=str,json";
	public static String uri2 = "kafka2://demo01:9192,demo02:9192,demo03:9192/?df=json";

	public static void main(String... args) throws IOException {
		System.setProperty("albacore.debug.suffix", "5");
		try (Connection c = Connection.connect(new URISpec(uri)); Input<Rmap> i = c.input("test02");) {
			i.open();
			while (!i.empty())
				i.dequeue(rs -> {
					rs.eachs(r -> {
						System.err.println(r.toString());
					});
				});
		}
	}
}
