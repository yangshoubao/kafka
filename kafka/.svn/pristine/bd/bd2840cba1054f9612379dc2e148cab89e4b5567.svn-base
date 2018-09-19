package com.kentrasoft.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Value("${spring.kafka.topic.nb-upload}")
    private String topicUpload;

    @Value("${spring.kafka.topic.nb-callback}")
    private String topicCallback;

    @Value("${spring.kafka.topic.nb-subscribe}")
    private String topicSubscribe;

    @Value("${spring.kafka.topic.nb-agps}")
    private String topicAgps;

    @Value("${spring.kafka.topic.nb-cd-upload}")
    private String topicCdUpload;

    /**
     * 发送消息到kafka
     * 主题：upload
     */
    public void sendToUpload(String message){
        if(log.isInfoEnabled()){
            log.info("发送消息到upload主题：" + message);
        }
        kafkaTemplate.send(topicUpload, message);
    }

    /**
     * 发送消息到kafka
     * 主题：callback
     */
    public void sendToCallback(String message){
        if(log.isInfoEnabled()){
            log.info("发送消息到callback主题：" + message);
        }
        kafkaTemplate.send(topicCallback, message);
    }

    /**
     * 发送消息到kafka
     * 主题：subscribe
     */
    public void sendToSubscribe(String message){
        if(log.isInfoEnabled()){
            log.info("发送消息到subscribe主题：" + message);
        }
        kafkaTemplate.send(topicSubscribe, message);
    }

    /**
     * 发送消息到kafka
     * 主题：subscribe
     */
    public void sendToAgps(String message){
        if(log.isInfoEnabled()){
            log.info("发送消息到agps主题：" + message);
        }
        kafkaTemplate.send(topicAgps, message);
    }

    /**
     * 发送消息到kafka
     * 主题：cd-upload
     */
    public void sendToCdUpload(String message){
        if(log.isInfoEnabled()){
            log.info("发送消息到cd-upload主题：" + message);
        }
        kafkaTemplate.send(topicCdUpload, message);
    }
}
