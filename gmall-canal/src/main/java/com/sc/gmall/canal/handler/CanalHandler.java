package com.sc.gmall.canal.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.google.common.base.CaseFormat;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.sc.gmall.canal.util.MyKafkaSender;
import com.sc.gmall.common.constant.GmallConstant;

import java.util.List;

/**
 * @Autor sc
 * @DATE 0009 14:16
 */
public class CanalHandler {
    public static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList){
        if("order_info".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
            //下单操作
            for (CanalEntry.RowData rowData : rowDatasList) {//行集展开
                List<CanalEntry.Column> columnsList = rowData.getAfterColumnsList();
                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : columnsList) {//列集展开
                    String columnName = column.getName();
                    String columnValue = column.getValue();
                    System.out.println(columnName+"::"+columnValue);
                    //驼峰转换
                    String propertiesName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    jsonObject.put(propertiesName,columnValue);
                }
                //发送给kafka
                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_NEW_ORDER,jsonObject.toString());
            }

        }
    }

}
