package com.sc.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

/**
 * @Autor sc
 * @DATE 0030 15:38
 */

@RestController
public class LoggerController {
    KafkaTemplate<String,String> KafkaTemplate;
    private static final org.slf4j.Logger logger =  LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("/log")
    public String dolog(@RequestParam("log") String logJson){
        //补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts",System.currentTimeMillis());
        //log落盘用log4j
        logger.info(jsonObject.toJSONString());
        //发送kafka

        //KafkaTemplate.send();


        //System.out.println(logJson);
        return "success";
    }

}
