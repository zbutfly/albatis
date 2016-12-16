package com.hzcominfo.albatis.search;

import java.io.IOException;

import com.hzcominfo.albatis.search.exception.SearchAPIException;
import com.hzcominfo.albatis.search.result.Result;

/**
 * Created by ljx on 2016/11/25.
 *
 * @author ljx
 * @date 2016/11/25 对操作的描述
 */
public interface Describe {
	Result execute() throws SearchAPIException, IOException;
}
