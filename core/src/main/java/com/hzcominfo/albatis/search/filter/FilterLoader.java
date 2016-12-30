package com.hzcominfo.albatis.search.filter;

import com.hzcominfo.albatis.search.exception.SearchAPIException;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

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


    /**
     * 通过指定的路径获取配置文件并构造生成
     *
     * @param path
     * @return
     */
    protected static Map<String, FilterChainConfig> XMLParser(final String path) {
        SAXBuilder builder = new SAXBuilder();
        Document document;
        Map<String, FilterChainConfig> map = null;
        try {
            InputStream inputStream = FilterLoader.class.getResourceAsStream(path);
            document = builder.build(inputStream);
            Element root = document.getRootElement();
            List<Element> chain = root.getChildren("filterChain");
            if (chain.size() == 0) {
                return null;
            }
            //进入处理逻辑
            map = new HashMap<>(chain.size() * 2);
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
                chainConfig.configs = new ArrayList<>(filters.size());
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
                    chainConfig.configs.add(config);
                }
                map.put(chainConfig.name, chainConfig);
            }
        } catch (JDOMException | IOException e) {
            e.printStackTrace();
        }
        return map;
    }


    /**
     * 主要的调用方法 通过传入的FilterChain接口实现类，确定配置的Filter接口
     * @param path 配置文件路径
     * @param <Q>
     * @param <R>
     * @return
     * @throws SearchAPIException
     */
    public static <Q, R> FilterChain<Q, R> invoke(String path) throws SearchAPIException {
        if (path == null) {
            path = "/searchFilterConfig-internal.xml";
        }
        String invokeClazz = getInvokedClazz(); //

        System.out.println(invokeClazz);

        return null;
    }

    /**
     * 使用默认路径调用 首先检查JVM配置，如果有search.filter.config，则按照配置路径调用
     * <p>如果未配置，则调用内部XML，文件名:search-filter-config.xml
     */
    public static <Q,R> FilterChain<Q, R> invoke() throws SearchAPIException {
        if(System.getProperty("search.filter.config")!=null){
            String path = System.getProperty("search.filter.config");
            if(!path.contains(".xml")){

            }
            return invoke(path);
        }
        return invoke("/searchFilterConfig-internal.xml");
    }





    static class FilterChainConfig {
        String name;
        String clazz;
        Map<String, String> params;
        List<FilterConfig> configs;
    }

    static class FilterConfig {
        String clazz;
        int order;
        Map<String, String> params;
    }

    public static void main(String[] args) {
       // XMLParser("/internal-filter.xml");
       // XMLParser("/internal-filter2.xml");

    }

    /**
     * 获取执行invoke方法的类名，
     * 考虑到调用invoker的类可能会被大量其他的类逐步调用，所以从最底部逐渐判断，直到第一个不为本类的名称为止
     * @return 类的全名
     */
    private static String getInvokedClazz(){
        Exception e = new Exception();
        for( StackTraceElement element:e.getStackTrace()){
            if(!element.getClassName().equals(FilterLoader.class.getCanonicalName())){
                return element.getClassName();
            }
        }
        return null;
    }
}
