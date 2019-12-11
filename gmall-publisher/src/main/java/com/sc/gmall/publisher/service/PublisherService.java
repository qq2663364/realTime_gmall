package com.sc.gmall.publisher.service;

import java.util.Map;

/**
 * @Autor sc
 * @DATE 0007 9:55
 */
public interface PublisherService {

    public Integer getDauTotal (String date);

    public Map getDauHourMap(String date);

    public Double getOrderAmount(String date);

    public Map getOrderAmountHourMap(String date);
}
