package com.sc.gmall.publisher.controller;
import java.util.ArrayList;
import java.util.HashMap;
import	java.util.Map;

import com.alibaba.fastjson.JSON;
import com.sc.gmall.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @Autor sc
 * @DATE 0007 9:55
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date")String date) {


        List<Map> totalList = new ArrayList<>();
        //新增日活
        HashMap dauMap = new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value",dauTotal);
        totalList.add(dauMap);

        //新增设备
        HashMap newMidMap = new HashMap();
        newMidMap.put("id","newmid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value",222);
        totalList.add(newMidMap);
        //转换为Json串
        return JSON.toJSONString(totalList);

    }

//    public static void main(String[] args) {
//        PublisherServiceImpl publisherService = new PublisherServiceImpl();
//        Integer dauTotal = publisherService.getDauTotal("2019-12-07");
//        System.out.println(dauTotal);
//    }

}
