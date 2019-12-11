package com.sc.gmall.publisher.controller;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.sc.gmall.publisher.bean.Option;
import com.sc.gmall.publisher.bean.OptionGroup;
import com.sc.gmall.publisher.bean.SaleDetailInfo;
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

    @GetMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,@RequestParam("startpage") int pageNo, @RequestParam("size") int pageSize ,@RequestParam("keyword") String keyword){
        Map saleDetailMapWithGender = publisherService.getSaleDetailMap(date, keyword, pageNo, pageSize, "user_gender", 2);
        Integer total =(Integer) saleDetailMapWithGender.get("total");
        List<Map> detailList =(List<Map>) saleDetailMapWithGender.get("detail");
        Map aggsMapGender =(Map) saleDetailMapWithGender.get("aggsMap");



        Long femaleCount = (Long)aggsMapGender.getOrDefault("F", 0);
        Long  maleCount = (Long)aggsMapGender.getOrDefault("M", 0);

        Double maleRatio=   Math.round( maleCount*1000D /total)/10D;
        Double femaleRatio=   Math.round( femaleCount*1000D /total)/10D;
        List<Option> optionListGender=new ArrayList<>();
        optionListGender.add(new Option("男", maleRatio) );
        optionListGender.add(new Option("女", femaleRatio) );

        OptionGroup optionGroupGender = new OptionGroup("性别占比",optionListGender);

        Map saleDetailMapWithAge = publisherService.getSaleDetailMap(date, keyword, pageNo, pageSize, "user_age", 100);
        Map aggsMapAge =(Map) saleDetailMapWithAge.get("aggsMap");

        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;


        for (Object o : aggsMapAge.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String age = (String)entry.getKey();
            Long ageCount =(Long) entry.getValue();
            if(Integer.parseInt(age)<20){
                age_20count+=ageCount;
            }else if(Integer.parseInt(age)>=20&&Integer.parseInt(age)<=30){
                age20_30count+=ageCount;
            }else{
                age30_count+=ageCount;
            }
        }
        //各年龄段的占比
        Double age_20Ratio= Math.round( age_20count*1000D /total)/10D;
        Double age20_30Ratio=Math.round( age20_30count*1000D /total)/10D;
        Double age30_Ratio=Math.round( age30_count*1000D /total)/10D;

        List<Option> optionListAge=new ArrayList<>();
        optionListAge.add(new Option("20岁以下", age_20Ratio) );
        optionListAge.add(new Option("20岁-30岁", age20_30Ratio) );
        optionListAge.add(new Option("30岁以上", age30_Ratio) );
        OptionGroup optionGroupAge = new OptionGroup("年龄占比",optionListAge);


        List<OptionGroup> optionGroups=new ArrayList<>();
        optionGroups.add(optionGroupGender);
        optionGroups.add(optionGroupAge);

        SaleDetailInfo saleDetailInfo = new SaleDetailInfo(total, optionGroups, detailList);

        return JSON.toJSONString(saleDetailInfo);
    }

}
