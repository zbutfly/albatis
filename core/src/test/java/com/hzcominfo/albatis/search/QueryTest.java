package com.hzcominfo.albatis.search;

import com.hzcominfo.albatis.search.exception.SearchAPIException;
import com.hzcominfo.albatis.search.filter.FilterLoader;

/**
 * Created by ljx on 2016/11/23.
 */

public class QueryTest {
	// @Test
	// public void createQueryTest() {
	//
	// try (Connection connection = BuildConnection.createConnection("aaa")) {
	// and(CalculusBuild.allField("aa"), equal("name", "taome"));
	// Criteria c = and(allField("zz"), or(equal("name", "taome")));
	// Criteria x = and(c);
	// String[] testStr = new String[2];
	// connection.getQuery().select(column("aa"),column("bb")).db("index1","index2").from(table("table"),
	// table("2")).where(c)
	// .limit(100L).skip(1000L)
	// .orderBy("field", OrderBy.Order.ASC)
	// .orderBy("field", OrderBy.Order.ASC).execute();
	//
	//
	//
	// } catch (SearchAPIException e) {
	// e.printStackTrace();
	// } catch (URISyntaxException e) {
	// e.printStackTrace();
	// }
	//
	// }


	public static void test() throws SearchAPIException {
		FilterLoader.invokeOf(null);
	}
}