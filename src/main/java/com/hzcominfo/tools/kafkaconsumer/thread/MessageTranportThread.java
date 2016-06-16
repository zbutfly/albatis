/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.hzcominfo.tools.kafkaconsumer.thread;

import com.hzcominfo.tools.kafkaconsumer.context.DataContext;
import com.hzcominfo.tools.kafkaconsumer.mapper.KafkaMessage;
import com.hzcominfo.tools.kafkaconsumer.mapper.KafkaTopicConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hzcominfo
 */
public class MessageTranportThread extends Thread {

    private boolean mixFlag;
    private int buffNum;
    private int buffMax;
    int noDataCount = 0;
    boolean newListFlag = false;
    List<KafkaMessage> workList;
    private KafkaTopicConfig[] topics;
    public boolean stopFlag = false;

    public void setTopics(KafkaTopicConfig[] topics) {
        this.topics = topics;
    }

    public void setMixFlag(boolean mixFlag) {
        this.mixFlag = mixFlag;
    }

    public void setBuffNum(int buffNum) {
        this.buffNum = buffNum;
    }

    public void setBuffMax(int buffMax) {
        this.buffMax = buffMax;
    }

    @Override
    public void run() {

        if (mixFlag) {
            List<KafkaMessage> msgList = Collections.synchronizedList(new ArrayList<KafkaMessage>());
            DataContext.msgPcgList.add(msgList);
            workList = DataContext.msgPcgList.get(0);
        } else {
            for (KafkaTopicConfig topic : topics) {
                DataContext.msgMap.put(topic.getTopic(), Collections.synchronizedList(new ArrayList<KafkaMessage>()));
            }
        }

        while (stopFlag == false) {
            if (!DataContext.msgList.isEmpty()) {
                KafkaMessage km = DataContext.msgList.remove(0);
                if (mixFlag) {
                    while (transportDataToMixList(km) == false) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ex) {
                            Logger.getLogger(MessageTranportThread.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
                } else {
                    if (transportDataToNoMixMap(km) == false) {
                        DataContext.msgList.add(km);
                    }
                }

            } else {
                if (noDataCount < buffMax) {
                    noDataCount++;
                } else {
                    newListFlag = true;
                }

            }
        }
    }

    /**
     * set message to the mix queue
     *
     * @param km one message
     * @return true:add queue success|false:queue is full
     */
    public boolean transportDataToMixList(KafkaMessage km) {
        boolean flag = false;
        if (workList.size() < buffMax) {
            workList.add(km);
            flag = true;
        } else {
            if (DataContext.msgPcgList.size() < buffNum) {
                List<KafkaMessage> list = new ArrayList<>();
                DataContext.msgPcgList.add(list);
                workList = list;
                workList.add(km);
                flag = true;
            }
        }

        if (newListFlag) {
            if (DataContext.msgPcgList.size() < buffNum) {
                List<KafkaMessage> list = Collections.synchronizedList(new ArrayList<KafkaMessage>());
                DataContext.msgPcgList.add(list);
                workList = list;
            }
            //空闲状态归0
            newListFlag = false;
            noDataCount = 0;
        }

        return flag;
    }

    /**
     * set message to message map
     *
     * @param km
     * @return ture:set success|false
     */
    public boolean transportDataToNoMixMap(KafkaMessage km) {
        boolean flag = false;
        workList = DataContext.msgMap.get(km.getTopic());

        if (workList.size() < buffMax) {
            workList.add(km);
            flag = true;
        }

        return flag;
    }

}
