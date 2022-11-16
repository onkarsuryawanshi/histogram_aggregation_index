package com.elastiSearchClient.repository;

import com.elastiSearchClient.client.LocalHostClient;
import com.elastiSearchClient.config.ES_config;
import com.elastiSearchClient.exception.InvalidInputException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class AggregationRepository {
    @Autowired
    LocalHostClient restHighLevelClient;
    RestHighLevelClient client = restHighLevelClient.create();

    @Autowired
    private ElasticSearchRepository query;


    public Object getAggByUserField(String startDate, String endDate, String field, String operation) {

        switch (operation.toUpperCase()) {
            case "SUM":
                return getSumOfDataUsedByUserField(startDate, endDate,field);
            case "AVG":
                return getAvgOfDataUsedByUserField(startDate, endDate,field);
            case "MAX":
                return getMaxOfDataUsedByUserField(startDate, endDate,field);
            case "MIN":
                return getMinOfDataUsedByUserField(startDate, endDate,field);
        }
        return null;
    }

    private Object getMinOfDataUsedByUserField(String startDate, String endDate, String userField) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        MinAggregationBuilder aggregation = AggregationBuilders.min("MIN").field(userField);
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidInputException("error in input can't find the min of entered field...Please try with different field");
        }
        Double minOfUserField = null;
        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();

        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            minOfUserField = obj.getDouble("value");
            System.out.println("min Of data Used in 2 between 2 dates ==> " + minOfUserField);
        }

        System.out.println("Document Who Used min data is ==>"+query.executeMatchQuery("DataUsed", String.valueOf(minOfUserField)));

        return getDevice(minOfUserField,userField);
    }



    private SearchResponse getDevice(Double maxDataUsed, String userField) {

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(userField, maxDataUsed));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException();
        }

        return searchResponse;
    }
    private Object getMaxOfDataUsedByUserField(String startDate, String endDate, String userField) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        MaxAggregationBuilder aggregation = AggregationBuilders.max("MAX").field(userField);
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidInputException("error in input can't find max for entered field...Please try with different field");
        }
        Double maxOfField = null;
        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();

        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            maxOfField = obj.getDouble("value");
            System.out.println("Max Of data Used in 2 between 2 dates ==> " + maxOfField);
        }


        System.out.println("Document Who Used max data is ==>"+query.executeMatchQuery("DataUsed", String.valueOf(maxOfField)));

        return getDevice(maxOfField,userField);
    }

    private Double getAvgOfDataUsedByUserField(String startDate, String endDate, String userField) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AvgAggregationBuilder aggregation = AggregationBuilders.avg("AVG").field(userField);
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidInputException("error in field, can't find avg for field ...Please try with different field");
        }
        Double avgOfField = null;
        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();

        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            avgOfField = obj.getDouble("value");
            System.out.println("Average Of data Used in 2 between 2 dates ==> " + avgOfField);
        }
        return avgOfField;
    }

    private Double getSumOfDataUsedByUserField(String startDate, String endDate, String userField) {
        
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        SumAggregationBuilder aggregation = AggregationBuilders.sum("sum").field(userField);
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidInputException("error in input field entered ...Please try with different field");
        }
        Double sumOfField = null;
        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();

        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            sumOfField = obj.getDouble("value");
            System.out.println("sum Of data Used in 2 between 2 dates ==> " + sumOfField);
        }
        return sumOfField;
        
    }
}
