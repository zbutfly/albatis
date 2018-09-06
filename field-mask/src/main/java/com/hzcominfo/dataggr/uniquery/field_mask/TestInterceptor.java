package com.hzcominfo.dataggr.uniquery.field_mask;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import com.hzcominfo.dataggr.uniquery.Client;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;
import com.hzcominfo.dataggr.uniquery.field_maskBean.FieldBeanX;

import net.butfly.albacore.io.URISpec;
import net.sf.jsqlparser.statement.execute.Execute;
import scala.annotation.meta.field;

public abstract class TestInterceptor implements HandlerInterceptor {

	private static final Logger logger = LoggerFactory.getLogger(TestInterceptor.class);

	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
			throws Exception {
		return true;
	}

	// 该方法将在请求处理之后，可以在这个方法中对Controller处理之后的ModelAndView对象进行操作。
	@Override
	public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler,
			ModelAndView result) throws Exception {
		Client client = new Client(new URISpec(""));
		ResultSet execute = (ResultSet) client.execute("sql", new Object[0]);
		List<Map<String, Object>> results = execute.getResults();
		
		for (Map<String, Object> map : results) {
		if (map.containsKey("zjhm")) {
			map.remove("zjhm");
		}

		}
		
	}

	@Override
	public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
			throws Exception {

	}
}
