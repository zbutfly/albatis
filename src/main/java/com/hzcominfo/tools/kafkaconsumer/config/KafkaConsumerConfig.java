/*
 * To change this license header, choose License Headers in Project Properties_
 * To change this template file, choose Tools | Templates
 * and open the template in the editor_
 */
package com.hzcominfo.tools.kafkaconsumer.config;

/**
 *
 * @author hzcominfo
 */
public class KafkaConsumerConfig {

    /**
     * zookeeper connect address,default:""
     */
    public String zookeeper_connect = "";

    /**
     * zookeeper connection time out(ms),dafault:15000ms
     */
    public int zookeeper_connection_timeout_ms = 15000;

    /**
     * zookeeper sync time(ms),default:5000ms
     */
    public int zookeeper_sync_time_ms = 5000;

    /**
     * kafka group id,default:""
     */
    public String group_id = "";

    /**
     * auto commit,default:false
     */
    public boolean auto_commit_enable = false;

    /**
     * auto commit interval time(ms),default:1000
     */
    public int auto_commit_interval_ms = 1000;
    public String auto_offset_reset = "smallest";

    /**
     * session time out(ms),default:30000ms
     */
    public int session_timeout_ms = 30000;
    public String partition_assignment_strategy = "range";
    /**
     * receive buffer(byte),default:5m
     */
    public int socket_receive_buffer_bytes = 5 * 1024 * 1024;
    /**
     * fetch max size(byte),default:3m
     */
    public int fetch_message_max_bytes = 3 * 1024 * 1024;

}
