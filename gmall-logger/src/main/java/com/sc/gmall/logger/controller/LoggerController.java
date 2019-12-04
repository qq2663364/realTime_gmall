package com.sc.gmall.logger.controller;
import	java.awt.Desktop.Action;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sc.gmall.common.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/**
 * @Autor sc
 * @DATE 0030 15:38
 */

@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String, String> KafkaTemplate;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("/log")
    public String dolog(@RequestParam("log") String logJson) {
        //补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());
        //log落盘用log4j
        logger.info(jsonObject.toJSONString());
        //发送kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            KafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP,jsonObject.toJSONString());
        } else {
            KafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT,jsonObject.toJSONString());
        }


        //System.out.println(logJson);
        return "success";

    }

}
