package com.sc.gmall.publisher.service.impl;

import com.sc.gmall.common.constant.GmallConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
        TermsBuilder aggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(25);
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
}
