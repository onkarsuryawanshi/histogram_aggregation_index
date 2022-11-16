package com.elastiSearchClient.repository;

import com.elastiSearchClient.client.LocalHostClient;
import com.elastiSearchClient.config.ES_config;
import com.elastiSearchClient.exception.InvalidInputException;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class ElasticSearchRepository {
    @Autowired
    LocalHostClient restHighLevelClient;
    RestHighLevelClient client = restHighLevelClient.create();
    public SearchResponse getAllDocOnIndex() {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return searchResponse;

    }

    public long getDocCountOnIndex() {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        SearchHits hits = searchResponse.getHits();
        TotalHits totalHits = hits.getTotalHits();

        long numHits = totalHits.value;
        return numHits;
    }

    public SearchResponse executeMatchQuery(String field, String textTobeSearch) {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field, textTobeSearch));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidInputException("error in input ...Please try with different field");
        }

        System.out.println("total number of match record are ==> " + searchResponse.getHits().getTotalHits());
        System.out.println(searchResponse);
        return searchResponse;
    }

    public Double getSumOfDataUserd(String startDate, String endDate) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);

        SumAggregationBuilder aggregationBuilders = AggregationBuilders.sum("SUM").field("DataUsed");


        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);

        SumAggregationBuilder aggregation = AggregationBuilders.sum("sum").field("DataUsed");
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Double sumDataUsed = null;
        /*
         * from here return only value
         * */
        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();

        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            sumDataUsed = obj.getDouble("value");
            System.out.println("value" +sumDataUsed);
        }


//        return searchResponse;
        return sumDataUsed;
    }

    public Double getAvgOfDataUsed(String startDate, String endDate) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);

        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);


        AvgAggregationBuilder aggregation = AggregationBuilders.avg("avg").field("DataUsed");
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();
        Double avgDataUsed = null;

        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            avgDataUsed = obj.getDouble("value");
            System.out.println("value" +avgDataUsed);
        }

//        return searchResponse;
        return avgDataUsed;
    }

    public SearchResponse getDeviceWhoUsedMaxData(String startDate, String endDate) {


        Double maxDataUsed = null;
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        searchSourceBuilder.query(rangeQueryBuilder);

        MaxAggregationBuilder aggregation = AggregationBuilders.max("max").field("DataUsed");
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(searchResponse);
        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();

        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            maxDataUsed = obj.getDouble("value");
            System.out.println("value" + maxDataUsed);
        }
        //private method
        return getDevice(maxDataUsed);
    }
    private SearchResponse getDevice(Double maxDataUsed) {

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("DataUsed", maxDataUsed));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException();
        }

        return searchResponse;
    }

    public SearchResponse getDeviceWhoUsedMinData(String startDate, String endDate) {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();


        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        searchSourceBuilder.query(rangeQueryBuilder);

        MinAggregationBuilder aggregation = AggregationBuilders.min("Min").field("DataUsed");
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();
        Double minDataUsed = null;
        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            minDataUsed = obj.getDouble("value");
            System.out.println("min data used is ==> " + minDataUsed);
        }
        //private method
        return getDevice(minDataUsed);
    }

    public SearchResponse getSumForSpecificField(String startDate, String endDate, String userField) {
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
            throw new InvalidInputException("error in input ...Please try with different field");
        }

        Aggregations agg = searchResponse.getAggregations();
        return searchResponse;
    }


    public Double getAvgForSpecificField(String startDate, String endDate, String userField) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
//        AggregationBuilders aggregation = AggregationBuilders

        SumAggregationBuilder aggregation = AggregationBuilders.sum("Avg").field(userField);
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
//        *********************************
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidInputException("error in input ...Please try with different field");
        }

        Map<String, Aggregation> agg = searchResponse.getAggregations().asMap();
        Double avgDataUsed = null;
        for (Map.Entry<String, Aggregation> curr : agg.entrySet()
        ) {
            JSONObject obj = new JSONObject(curr.getValue());
            avgDataUsed = obj.getDouble("value");
            System.out.println("min data used is ==> " + avgDataUsed);
        }
        return avgDataUsed;
    }

    public GetResponse getDocById(String doc_id) {
        GetRequest getRequest = new GetRequest(
                ES_config.indexName,
                doc_id);

        GetResponse response = null;
        try {
            response = client.get(getRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    public MainResponse getInfoLog() {
        MainResponse response = null;
        try {
            response = client.info(RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    public List<String> getRadioType() {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(QueryBuilders.matchQuery("radioType",inputRadioType));
        AggregationBuilder aggregation =
                AggregationBuilders
                        .filters("agg",
                                new FiltersAggregator.KeyedFilter("BN", QueryBuilders.matchQuery("radioType", "BN")),
                                new FiltersAggregator.KeyedFilter("RN", QueryBuilders.matchQuery("radioType", "RN")));
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidInputException("error in input ...Please try with different field");
        }

        Filters agg = searchResponse.getAggregations().get("agg");
        List<String> aggList = new ArrayList<>();
        // For each entry
        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            aggList.add("key ==> " + key + "    doc_count ==>" + docCount);
        }
        System.out.println(searchResponse);
        return aggList;
    }

    public Map<String, Long> getAggregationByOperators() {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation =AggregationBuilders
                .terms("operators")
                .field("operator.keyword");
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException();
        }

        Terms genders = searchResponse.getAggregations().get("operators");

        Map<String,Long> mapAggByOperator = new HashMap<>();
        for (Terms.Bucket entry : genders.getBuckets()) {
            mapAggByOperator.put((String) entry.getKey(),entry.getDocCount());
        }
        return mapAggByOperator;
    }

    public Map<String, Long> termAggByField(String field) {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation =AggregationBuilders
                .terms(field)
                .field(field+".keyword");
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException();
        }

        Terms genders = searchResponse.getAggregations().get(field);

        // For each entry
        Map<String,Long> mapAggByOperator = new HashMap<>();
        for (Terms.Bucket entry : genders.getBuckets()) {
            mapAggByOperator.put((String) entry.getKey(),entry.getDocCount());
        }
        return mapAggByOperator;
    }

    public Map<Map<String, Double>, Double> getHistogramByDateMaxDataUsed() {
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .calendarInterval(DateHistogramInterval.MONTH)
                        .subAggregation(AggregationBuilders
                                .max("max")
                                .field("DataUsed")
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new RuntimeException();
        }
        System.out.println(searchResponse);
        Histogram agg = searchResponse.getAggregations().get("agg");
        Double maxOfDataUsed = null;
        Map<Map<String,Double>,Double> mapOfMaxValueHistogram = new HashMap<>();
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            Map<String,Double> map1 = new HashMap<>();
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            double docCount = agg.getBuckets().get(i).getDocCount();
            map1.put(keyAsString,docCount);
            Map<String, Aggregation> agg1 = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : agg1.entrySet()
            ) {
                JSONObject obj = new JSONObject(curr.getValue());
                maxOfDataUsed = obj.getDouble("value");
                mapOfMaxValueHistogram.put(map1,maxOfDataUsed);
                System.out.println("Max Of data Used in 2 between 2 dates ==> " + maxOfDataUsed);
            }
        }

        return mapOfMaxValueHistogram;
    }

    public boolean DeleteByIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest(ES_config.indexName);
        AcknowledgedResponse deleteIndexResponse = null;
        try {
            deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException();}
            catch (ElasticsearchException e){
                return false;
            }
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        return acknowledged;
    }
}
