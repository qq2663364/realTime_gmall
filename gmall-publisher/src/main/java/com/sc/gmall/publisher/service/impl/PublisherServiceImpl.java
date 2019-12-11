package com.sc.gmall.publisher.service.impl;

import com.sc.gmall.common.constant.GmallConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Autor sc
 * @DATE 0007 9:57
 */
@Service
public class PublisherServiceImpl implements com.sc.gmall.publisher.service.PublisherService {
    @Autowired
    JestClient jestClient;

    @Override
    public Integer getDauTotal(String date) {
        //第一种构建ES查询
//        String query = "GET gmall1205_dau/_search\n" +
//                "{\n" +
//                "  \"query\": {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\":{\n" +
//                "          \"logDate\":\"2019-12-07\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }" +
//                "}";

        //第二种构建ES查询
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();
        Integer total = 0;
        try {
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return total;
    }

    @Override
    public Map getDauHourMap(String date) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(aggsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType("_doc").build();

        HashMap dauHourMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                String key = bucket.getKey();
                Long value = bucket.getCount();
                dauHourMap.put(key,value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dauHourMap;
    }

    @Override
    public Double getOrderAmount(String date) {


        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        SumBuilder sumBuilder = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        searchSourceBuilder.aggregation(sumBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_NEW_ORDER).addType("_doc").build();
        Double sum_totalamount = 0D;
        try {
            SearchResult searchResult = jestClient.execute(search);
            sum_totalamount = searchResult.getAggregations().getSumAggregation("sum_totalAmount").getSum();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sum_totalamount;
    }

    @Override
    public Map getOrderAmountHourMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);

        SumBuilder sumBuilder = AggregationBuilders.sum("sum_toalamount").field("totalAmount");
        //子聚合
        termsBuilder.subAggregation(sumBuilder);

        searchSourceBuilder.aggregation(termsBuilder);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_NEW_ORDER).addType("_doc").build();
        HashMap<String,Double> hourMap = new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> groupby_createHour = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : groupby_createHour) {
                String hourKey = bucket.getKey();
                Double hourAmount = bucket.getSumAggregation("sum_toalamount").getSum();
                hourMap.put(hourKey,hourAmount);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return hourMap;
    }

    @Override
    public Map getSaleDetailMap(String date, String keyword, int pageNo, int pageSize, String aggsFieldName, int aggsSize) {
        Integer total=0;
        List<Map> detailList=new ArrayList<>();
        Map<String ,Long> aggsMap=new HashMap<>();

        Map saleMap=new HashMap();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        //过滤
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        //全文匹配
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));


        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_" + aggsFieldName).field(aggsFieldName).size(aggsSize);
        searchSourceBuilder.aggregation(termsBuilder);

        //分页
        searchSourceBuilder.from((pageNo-1)*pageSize);
        searchSourceBuilder.size(pageSize);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE_DETAIL).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
            //明细
            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }
            //取聚合结果
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggsFieldName).getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                aggsMap.put( bucket.getKey(),bucket.getCount());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        saleMap.put("total",total);
        saleMap.put("detail",detailList);
        saleMap.put("aggsMap",aggsMap);


        return saleMap;
    }
}
