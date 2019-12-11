package com.sc.gmall.canal.app;

import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sc.gmall.canal.handler.CanalHandler;

/**
 * @Autor sc
 * @DATE 0009 13:39
 */
public class CanalApp {
    public static void main(String[] args) {
        //创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("centos01", 11111), "example", "", "");        //抓取
        while (true) {
            canalConnector.connect();
            canalConnector.subscribe("gmall.order_info");   //
            Message message = canalConnector.get(100);
            int size = message.getEntries().size();

            if (size == 0) {
                System.out.println("5");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //判断时间类型,只处理行变化业务
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //反序列化
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //获得行集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //表名
                        String tableName = entry.getHeader().getTableName();

                        CanalHandler.handle(tableName, eventType, rowDatasList);
                    }
                }
            }
        }
    }
}