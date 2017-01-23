package com.hzcominfo.albatis.search;

import com.hzcominfo.albatis.search.exception.SearchAPIException;

/**
 * Created by lic on 2016/12/29.
 */
public class InvokeTest {

	public static void main(String[] args) {
		try {
			QueryTest.test();
		} catch (SearchAPIException e) {
			e.printStackTrace();
		}
	}
}
