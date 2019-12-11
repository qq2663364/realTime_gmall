package com.sc.gmall.publisher.service;

import java.util.Map;

/**
 * @Autor sc
 * @DATE 0007 9:55
 */
public interface PublisherService {
    /**
     * 获取日活总数
     * @param date
     * @return
     */
    public Integer getDauTotal (String date);

    /**
     * 获取每小时活跃用户
     * @param date
     * @return
     */
    public Map getDauHourMap(String date);

    /**
     * 获取日期订单总数
     * @param date
     * @return
     */

    public Double getOrderAmount(String date);

    /**
     * 获取每小时订单总数
     * @param date
     * @return
     */
    public Map getOrderAmountHourMap(String date);

    /**
     *
     * @param date   日期
     * @param keyword      关键词
     * @param pageNo    页码
     * @param pageSize  页尺寸
     * @param aggsFieldName 聚合字段
     * @param aggsSize  聚合大小
     * @return
     */
    public Map  getSaleDetailMap(String date ,String keyword,int pageNo,int pageSize, String aggsFieldName,int aggsSize );
}
