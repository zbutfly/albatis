/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hzcominfo.tools.kafkaconsumer.context;

/**
 *
 * @author hzcominfo
 */
public class ResultContext {

    /**
     * 通用result-success
     */
    public static final String RESULT_SUCCESS = "SUCCESS";

    /**
     * 初始化result-already run
     */
    public static final String RESULT_INIT_ALREADY_RUN = "ALREADY RUNNING";

    /**
     * 初始化result-topics null
     */
    public static final String RESULT_INIT_NONE_TOPICS = "NONE TOPICS";

    /**
     * 初始化result-kafka config null
     */
    public static final String RESULT_INIT_CONFIG_NULL = "NO CONNECT ADDRESS OR NO GROUP ID";

    /**
     * 初始化result-zookeeper 连接不上
     */
    public static final String RESULT_INIT_CONN_ERROR = "ZOOKEEPER CONNECT UNSUCCESS";
}
