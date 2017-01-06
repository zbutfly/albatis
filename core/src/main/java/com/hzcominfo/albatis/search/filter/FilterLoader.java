package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.Query;
import com.hzcominfo.albatis.search.exception.SearchAPIException;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 通过解析XML文件获取Filter配置的工具类
 * <p>
 * 最简化的通过反射构造并注入参数的过程
 */
public abstract class FilterLoader {
    public static List<FilterChainConfig> configList;

    /**
     * 通过指定的路径获取配置文件并构造生成
     *
     * @param path
     * @return
     */

    static {//载入类的时候必然会进入本方法
        //包含锁的两步验证
        if (configList == null || configList.size() == 0) {
            synchronized (FilterLoader.class) {
                if (configList == null || configList.size() == 0) {
                    try {
                        String path;
                        if (System.getProperty("search.filter.config") != null) {
                            path = System.getProperty("search.filter.config");
                            if (!path.contains(".xml")) {
                                throw new SearchAPIException("");
                            }
                        } else {
                            path = "search-filter-config.xml";
                        }
                        //文件的存在性由Parser来判断
                        configList = XMLParser(path);
                    } catch (SearchAPIException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    protected static List<FilterChainConfig> XMLParser(final String path) throws SearchAPIException {
        if (path == null || path.isEmpty()) {
            throw new SearchAPIException("NULL PATH");
        }
        SAXBuilder builder = new SAXBuilder();
        Document document;
        List<FilterChainConfig> result = null;

        try {
            InputStream inputStream = FilterLoader.class.getResourceAsStream(path);

            if(inputStream==null){
                String p = FilterLoader.class.getResource("").getPath();
                File file = new File(p+path);
                if(file.exists()){
                    inputStream = new FileInputStream(file);
                }
            }



            document = builder.build(inputStream);
            Element root = document.getRootElement();
            List<Element> chain = root.getChildren("filterChain");
            if (chain.size() == 0) {
                return null;
            }
            //进入处理逻辑
            result = new ArrayList<>(chain.size());
            for (Element element : chain) {
                FilterChainConfig chainConfig = new FilterChainConfig();
                if (element.getChildren("param") != null
                        && element.getChildren("param").size() > 0) {
                    chainConfig.params = new HashMap<>();
                    for (Element param : element.getChildren("param")) {
                        chainConfig.params.put(
                                param.getAttributeValue("key"),
                                param.getAttributeValue("value")
                        );
                    }
                }
                Attribute clazz = element.getAttribute("class");
                if (clazz != null) {
                    chainConfig.clazz = clazz.getValue();
                }
                Attribute name = element.getAttribute("name");
                if (name != null) {
                    chainConfig.name = name.getValue();
                }
                List<Element> filters = element.getChildren("filter");
                chainConfig.filterConfigs = new ArrayList<>(filters.size());
                for (Element el : filters) {
                    FilterConfig config = new FilterConfig();
                    if (el.getChildren("param") != null
                            && element.getChildren("param").size() > 0) {
                        config.params = new HashMap<>();
                        for (Element param : element.getChildren("param")) {
                            config.params.put(
                                    param.getAttributeValue("key"),
                                    param.getAttributeValue("value")
                            );
                        }
                    }
                    Attribute clazzEl = el.getAttribute("class");
                    if (clazzEl != null) {
                        config.clazz = clazzEl.getValue();
                    }
                    Attribute order = el.getAttribute("order");
                    if (order != null) {
                        config.order = order.getIntValue();
                    }
                    chainConfig.filterConfigs.add(config);
                }
                if(chainConfig.filterConfigs.size()>0){
                    result.add(chainConfig);
                }
            }
        } catch (JDOMException | IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    /**
     * 主要的调用方法 通过传入的FilterChain接口实现类，确定配置的Filter接口
     *
     * @param clazz 待注入的类名
     * @param name  注册名
     * @param <Q>   Q
     * @param <R>   R
     * @return filterChain
     * @throws SearchAPIException 异常
     */
    @SuppressWarnings("unchecked")
    public static <Q, R> FilterChain<Q, R> invokeOf(Class clazz, String name) throws SearchAPIException {
        FilterChainConfig ret = null;
        for (FilterChainConfig config : configList) {
            if (config.name != null && config.name.equals(name)) {
                ret = config;
            }
        }
        if (ret == null) {
            for (FilterChainConfig config : configList) {
                if (config.invoke != null
                        && config.invoke.equals(clazz.getCanonicalName())) {
                    ret = config;
                }
            }
        }
        if(ret == null){
            return null;
        }
        if(ret.filterConfigs.size()==0){
            return null;   //在构建的时候就要检查，这里仅用做验证
        }
        try {
            Class invoke = Class.forName(ret.clazz);
            FilterChain filterChain = (FilterChain)invoke.getConstructor().newInstance(new Object());
            for(FilterConfig config : ret.filterConfigs){
                try {
                    if(config.clazz!=null){
                        Class f = Class.forName(config.clazz);
                        Filter filter = (Filter) f.getConstructor(new Class[]{}).newInstance(new Object());
                        filterChain.add(filter);
                    }
                }catch (Exception e){
                    continue;
                }
            }
            return filterChain;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 通过调用的类名获取应该注入的filterChain 如果存在多个，匹配第一个
     *
     * @param clazz 类型名
     * @param <Q>   Q
     * @param <R>   R
     * @return filterChain
     * @throws SearchAPIException 异常
     */
    public static <Q, R> FilterChain<Q, R> invokeOf(Class clazz) throws SearchAPIException {
        return invokeOf(clazz, null);
    }

    /**
     * 无参数调用 通过Java自身解析
     * @param <Q> Q
     * @param <R> R
     * @return filterChain
     * @throws SearchAPIException 异常
     */
    public static <Q, R> FilterChain<Q, R> invokeOf() throws SearchAPIException {
        String className = getInvokedClazz();
        if(className!=null){
            try {
                Class clazz = Class.forName(className);
                return invokeOf(clazz, null);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static class FilterChainConfig {
        String name;
        String clazz;
        String invoke;
        Map<String, String> params;
        List<FilterConfig> filterConfigs;
    }

    private static class FilterConfig {
        String clazz;
        int order;
        Map<String, String> params;
    }

    /**
     * 获取执行invoke方法的类名，
     * 考虑到调用invoker的类可能会被大量其他的类逐步调用，所以从最底部逐渐判断，直到第一个不为本类的名称为止
     *
     * @return 类的全名
     */
    private static String getInvokedClazz() {
        Exception e = new Exception();
        for (StackTraceElement element : e.getStackTrace()) {
            if (!element.getClassName().equals(FilterLoader.class.getCanonicalName())) {
                return element.getClassName();
            }
        }
        return null;
    }


    private FilterChainConfig findByName(String name, List<FilterChainConfig> source) {
        for (FilterChainConfig config : source) {
            if (config.name != null && config.name.equals(name)) {
                return config;
            }
        }
        return null;
    }

    private FilterChainConfig findByInvoke(String invoke, List<FilterChainConfig> source) {
        for (FilterChainConfig config : source) {
            if (config.invoke != null && config.invoke.equals(invoke)) {
                return config;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        // XMLParser("/internal-filter.xml");
        // XMLParser("/internal-filter2.xml");


        try {
            FilterChain<Query, Query> aa = FilterLoader.invokeOf(FilterLoader.class, "1");
            FilterChain<Query, Query> aa2 = FilterLoader.invokeOf(FilterLoader.class, "2");
            Query a = null;
            aa.doFilter(a, a);

        } catch (SearchAPIException e) {
            e.printStackTrace();
        }

    }
}
