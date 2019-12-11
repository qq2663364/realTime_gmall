package com.sc.gmall.publisher.controller;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.sc.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Autor sc
 * @DATE 0007 9:55
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {


        List<Map> totalList = new ArrayList<>();

        //新增日活
        HashMap dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        Integer dauTotal = publisherService.getDauTotal(date);
        dauMap.put("value", dauTotal);
        totalList.add(dauMap);

        //新增设备
        HashMap newMidMap = new HashMap();
        newMidMap.put("id", "newmid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 222);
        totalList.add(newMidMap);


        //新增交易额
        HashMap orderAmountMap = new HashMap();
        orderAmountMap.put("id", "orderAmount");
        orderAmountMap.put("name", "新增交易");
        Double orderAmount = publisherService.getOrderAmount(date);
        orderAmountMap.put("value", orderAmount);
        totalList.add(orderAmountMap);

        //转换为Json串
        return JSON.toJSONString(totalList);

    }

    @GetMapping("realtime-hour")
    public String getHourTotal(@RequestParam String id, @RequestParam String date) {
        if ("dau".equals(id)) {
            //今天的明细
            Map dauHourTDMap = publisherService.getDauHourMap(date);
            //昨天的明细
            String yesterday = getYesterday(date);
            Map dauHourYDMap = publisherService.getDauHourMap(yesterday);
            HashMap hourMap = new HashMap();
            hourMap.put("today", dauHourTDMap);
            hourMap.put("yesterday", dauHourYDMap);
            return JSON.toJSONString(hourMap);
        } else if ("orderAmount".equals(id)) {
            //今天的明细
            Map orderAmountHourTDMap = publisherService.getOrderAmountHourMap(date);
            //昨天的明细
            String yesterday = getYesterday(date);
            Map orderAmountHourYDMap = publisherService.getOrderAmountHourMap(yesterday);

            HashMap hourMap = new HashMap();
            hourMap.put("today", orderAmountHourTDMap);
            hourMap.put("yesterday", orderAmountHourYDMap);
            return JSON.toJSONString(hourMap);
        }

        return null;

    }

    private String getYesterday(String today) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterday = "";
        try {
            Date todayDate = simpleDateFormat.parse(today);
            Date yesterdayDate = DateUtils.addDays(todayDate, -1);
            yesterday = simpleDateFormat.format(yesterdayDate);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterday;
    }


}
