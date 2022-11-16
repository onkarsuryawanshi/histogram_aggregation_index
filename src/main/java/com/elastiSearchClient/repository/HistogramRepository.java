package com.elastiSearchClient.repository;


import com.elastiSearchClient.client.LocalHostClient;
import com.elastiSearchClient.config.ES_config;
import com.elastiSearchClient.dto.Agg.*;
import com.elastiSearchClient.dto.*;
import com.elastiSearchClient.exception.InvalidFieldEnteredException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Repository
public class HistogramRepository {

    @Autowired
    LocalHostClient restHighLevelClient;
    RestHighLevelClient client = restHighLevelClient.create();

    public Object getAggregationOperationsByUserField(String startDate, String endDate, String operation, String field) {

        switch (operation.toUpperCase()) {
            case "SUM":
                return getSumDataUsedFromHistogramRange(startDate, endDate, field);
            case "AVG":
                return getAvgDataUsedFromHistogramRange(startDate, endDate, field);
            case "MAX":
                return getMaxDataUsedFromHistogramRange(startDate, endDate, field);
            case "MIN":
                return getMinDataUsedFromHistogramRange(startDate, endDate, field);
        }
        return null;
    }


    public Map<String, Map<String, Double>> getAggregationByPoint(String startDate, String endDate, Integer points) {
        Date date1 = new Date();
        Date date2 = new Date();
        try {
            date1 = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
            date2 = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        long dateBeforeInMs = date1.getTime();
        long dateAfterInMs = date2.getTime();
        System.out.println(dateBeforeInMs);
        System.out.println(dateAfterInMs);

        long timeDiff = Math.abs(dateAfterInMs - dateBeforeInMs);

        int hoursDiff = (int) TimeUnit.HOURS.convert(timeDiff, TimeUnit.MILLISECONDS);
        int div = hoursDiff / (points - 1);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("date")
                        .format("yyyy-MM-dd")
//                        .calendarInterval(DateHistogramInterval.MONTH)
                        .fixedInterval(DateHistogramInterval.hours(div))
//                        .minDocCount(1)
                        .subAggregation(AggregationBuilders
                                .stats("Status")
                                .field("DataUsed")
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            System.out.println(searchRequest);
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new RuntimeException();
        }
        Map<String, Map<String, Double>> mapOfValue = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            double docCount = agg.getBuckets().get(i).getDocCount();
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            Map<String, Double> aggregationMap = new HashMap<>();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                JSONObject obj = new JSONObject(curr.getValue());
                double sum = (double) obj.get("sum");
                double avg = (double) obj.get("avg");
                double min = (double) obj.get("min");
                double max = (double) obj.get("max");
                aggregationMap.put("doc_count", docCount);
                aggregationMap.put("sum", sum);
                aggregationMap.put("avg", avg);
                aggregationMap.put("min", min);
                aggregationMap.put("max", max);

            }
            mapOfValue.put(keyAsString, aggregationMap);
        }
        return mapOfValue;

    }


    private Object getMinDataUsedFromHistogramRange(String startDate, String endDate, String field) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .calendarInterval(DateHistogramInterval.MONTH)
                        .subAggregation(AggregationBuilders
                                .min("MIN")
                                .field(field)
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Try again with proper field ");
        }
        Histogram agg = searchResponse.getAggregations().get("agg");
        List<Object> listOfMin = new ArrayList<>();
        Double minOfDataUsed = null;
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            MonthStamp monthStamp = new MonthStamp();
            MinResponseAgg minResponseAgg = new MinResponseAgg();
            ZonedDateTime fromDate = (ZonedDateTime) agg.getBuckets().get(i).getKey();
            monthStamp.setFromDate(fromDate);
            monthStamp.setEndDate(fromDate.plusMonths(1));
            Long docCount = agg.getBuckets().get(i).getDocCount();
            minResponseAgg.setDocCount(docCount);
            Map<String, Aggregation> maxValueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : maxValueMap.entrySet()) {

                JSONObject obj = new JSONObject(curr.getValue());
                minOfDataUsed = obj.getDouble("value");
                minResponseAgg.setMin(minOfDataUsed);
                minResponseAgg.setMonthStamp(monthStamp);
            }
            listOfMin.add(minResponseAgg);
        }

        return listOfMin;
    }

    private Object getAvgDataUsedFromHistogramRange(String startDate, String endDate, String field) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .calendarInterval(DateHistogramInterval.MONTH)
                        .subAggregation(AggregationBuilders
                                .avg("AVG")
                                .field(field)
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Try again with proper field ");
        }
        List<Object> listOfAvg = new ArrayList<>();
        Histogram agg = searchResponse.getAggregations().get("agg");
        Map<Map<String, Long>, Double> mapOfDateAndAvgDataUsed = new HashMap<>();
        Double avgOfDataUsed = null;
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            MonthStamp monthStamp = new MonthStamp();
            AvgResponseAgg avgResponseAgg = new AvgResponseAgg();
            ;
            ZonedDateTime fromDate = (ZonedDateTime) agg.getBuckets().get(i).getKey();
            monthStamp.setFromDate(fromDate);
            monthStamp.setEndDate(fromDate.plusMonths(1));
            Long docCount = agg.getBuckets().get(i).getDocCount();
            avgResponseAgg.setDocCount(docCount);
            Map<String, Aggregation> maxValueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : maxValueMap.entrySet()) {
                JSONObject obj = new JSONObject(curr.getValue());
                avgOfDataUsed = obj.getDouble("value");
                avgResponseAgg.setAvg(avgOfDataUsed);
                avgResponseAgg.setMonthStamp(monthStamp);
            }
            listOfAvg.add(avgResponseAgg);
        }

        return listOfAvg;
    }

    private Object getSumDataUsedFromHistogramRange(String startDate, String endDate, String field) {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .order(BucketOrder.key(true))
                        .calendarInterval(DateHistogramInterval.MONTH)
                        .subAggregation(AggregationBuilders
                                .sum("SUM")
                                .field(field)
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Try again with proper field ");
        }

        System.out.println(searchResponse);
        List<Object> listOfObject = new ArrayList<>();
        Histogram agg = searchResponse.getAggregations().get("agg");

        Double sumOfDataUsed = null;
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            MonthStamp monthStamp = new MonthStamp();
            SumResponseAgg sumResponseAgg = new SumResponseAgg();
            ZonedDateTime fromDate = (ZonedDateTime) agg.getBuckets().get(i).getKey();
            monthStamp.setFromDate(fromDate);
            monthStamp.setEndDate(fromDate.plusMonths(1));
            Long docCount = agg.getBuckets().get(i).getDocCount();
            sumResponseAgg.setDocCount(docCount);
            Map<String, Aggregation> maxValueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : maxValueMap.entrySet()) {
                JSONObject obj = new JSONObject(curr.getValue());
                sumOfDataUsed = obj.getDouble("value");
                sumResponseAgg.setMonthStamp(monthStamp);
                sumResponseAgg.setSum(sumOfDataUsed);
                listOfObject.add(sumResponseAgg);
            }
        }
        return listOfObject;
    }

    private Object getMaxDataUsedFromHistogramRange(String startDate, String endDate, String field) {

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("agg")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .calendarInterval(DateHistogramInterval.MONTH)
                        .subAggregation(AggregationBuilders
                                .max("max")
                                .field(field)
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;

        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Try again with proper field ");
        }
        Histogram agg = searchResponse.getAggregations().get("agg");
        List<Object> listOfObject = new ArrayList<>();
        Map<Map<String, Long>, Double> mapOfDateAndMaxDataUsed = new HashMap<>();
        Double maxOfDataUsed = null;
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            MonthStamp monthStamp = new MonthStamp();
            MaxResponseAgg maxResponseAgg = new MaxResponseAgg();
            ZonedDateTime fromDate = (ZonedDateTime) agg.getBuckets().get(i).getKey();
            monthStamp.setFromDate(fromDate);
            monthStamp.setEndDate(fromDate.plusMonths(1));

            Long docCount = agg.getBuckets().get(i).getDocCount();
            maxResponseAgg.setDocCount(docCount);
            Map<String, Aggregation> maxValueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : maxValueMap.entrySet()) {
                JSONObject obj = new JSONObject(curr.getValue());
                maxOfDataUsed = obj.getDouble("value");
                maxResponseAgg.setMax(maxOfDataUsed);
                maxResponseAgg.setMonthStamp(monthStamp);
            }
            listOfObject.add(maxResponseAgg);
        }
        return listOfObject;
    }

    public Map<Map<String, Double>, Double> getAggByPointsAndOperations(String startDate, String endDate, Integer points, String operation) {


        switch (operation.toUpperCase()) {
            case "SUM":
                return getSumDataUsedOnPointAndOperation(startDate, endDate, points);
            case "AVG":
                return getAvgDataUsedOnPointAndOperation(startDate, endDate, points);
            case "MAX":
                return getMaxDataUsedOnPointAndOperation(startDate, endDate, points);
            case "MIN":
                return getMinDataUsedOnPointAndOperation(startDate, endDate, points);
        }
        return null;
    }

    private Map<Map<String, Double>, Double> getMinDataUsedOnPointAndOperation(String startDate, String endDate, Integer points) {
        Date date1 = new Date();
        Date date2 = new Date();
        try {
            date1 = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
            date2 = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        long dateBeforeInMs = date1.getTime();
        long dateAfterInMs = date2.getTime();
        long timeDiff = Math.abs(dateAfterInMs - dateBeforeInMs);
        int hoursDiff = (int) TimeUnit.HOURS.convert(timeDiff, TimeUnit.MILLISECONDS);
        int div = hoursDiff / (points - 1);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("date")
                        .format("yyyy-MM-dd")
//                        .calendarInterval(DateHistogramInterval.MONTH)
                        .fixedInterval(DateHistogramInterval.hours(div))
//                        .minDocCount(1)
                        .subAggregation(AggregationBuilders
                                .min("MIN")
                                .field("DataUsed")
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            System.out.println(searchRequest);
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new RuntimeException();
        }
        Map<Map<String, Double>, Double> mapValue = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            Map<String, Double> mapOfKeyAsStringDocCount = new HashMap<>();
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            double docCount = agg.getBuckets().get(i).getDocCount();
            mapOfKeyAsStringDocCount.put(keyAsString, docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                System.out.println(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                mapValue.put(mapOfKeyAsStringDocCount, (Double) obj.get("value"));

            }
        }
        return mapValue;
    }

    private Map<Map<String, Double>, Double> getMaxDataUsedOnPointAndOperation(String startDate, String endDate, Integer points) {
        Date date1 = new Date();
        Date date2 = new Date();
        try {
            date1 = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
            date2 = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        long dateBeforeInMs = date1.getTime();
        long dateAfterInMs = date2.getTime();
        long timeDiff = Math.abs(dateAfterInMs - dateBeforeInMs);
        int hoursDiff = (int) TimeUnit.HOURS.convert(timeDiff, TimeUnit.MILLISECONDS);
        int div = hoursDiff / (points - 1);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("date")
                        .format("yyyy-MM-dd")
//                        .calendarInterval(DateHistogramInterval.MONTH)
                        .fixedInterval(DateHistogramInterval.hours(div))
//                        .minDocCount(1)
                        .subAggregation(AggregationBuilders
                                .max("MAX")
                                .field("DataUsed")
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            System.out.println(searchRequest);
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new RuntimeException();
        }
        Map<Map<String, Double>, Double> mapValue = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            Map<String, Double> mapOfKeyAsStringDocCount = new HashMap<>();
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            double docCount = agg.getBuckets().get(i).getDocCount();
            mapOfKeyAsStringDocCount.put(keyAsString, docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                System.out.println(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                mapValue.put(mapOfKeyAsStringDocCount, (Double) obj.get("value"));

            }
        }
        return mapValue;
    }

    private Map<Map<String, Double>, Double> getAvgDataUsedOnPointAndOperation(String startDate, String endDate, Integer points) {
        Date date1 = new Date();
        Date date2 = new Date();
        try {
            date1 = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
            date2 = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        long dateBeforeInMs = date1.getTime();
        long dateAfterInMs = date2.getTime();
        long timeDiff = Math.abs(dateAfterInMs - dateBeforeInMs);
        int hoursDiff = (int) TimeUnit.HOURS.convert(timeDiff, TimeUnit.MILLISECONDS);
        int div = hoursDiff / (points - 1);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("date")
                        .format("yyyy-MM-dd")
//                        .calendarInterval(DateHistogramInterval.MONTH)
                        .fixedInterval(DateHistogramInterval.hours(div))
//                        .minDocCount(1)
                        .subAggregation(AggregationBuilders
                                .avg("AVG")
                                .field("DataUsed")
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            System.out.println(searchRequest);
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new RuntimeException();
        }
        Map<Map<String, Double>, Double> mapValue = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            Map<String, Double> mapOfKeyAsStringDocCount = new HashMap<>();
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            double docCount = agg.getBuckets().get(i).getDocCount();
            mapOfKeyAsStringDocCount.put(keyAsString, docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                System.out.println(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                mapValue.put(mapOfKeyAsStringDocCount, (Double) obj.get("value"));

            }
        }
        return mapValue;
    }

    private Map<Map<String, Double>, Double> getSumDataUsedOnPointAndOperation(String startDate, String endDate, Integer points) {
        Date date1 = new Date();
        Date date2 = new Date();
        try {
            date1 = new SimpleDateFormat("yyyy-MM-dd").parse(startDate);
            date2 = new SimpleDateFormat("yyyy-MM-dd").parse(endDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        long dateBeforeInMs = date1.getTime();
        long dateAfterInMs = date2.getTime();
        long timeDiff = Math.abs(dateAfterInMs - dateBeforeInMs);
        int hoursDiff = (int) TimeUnit.HOURS.convert(timeDiff, TimeUnit.MILLISECONDS);
        int div = hoursDiff / (points - 1);
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("date").gt(startDate).lt(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("date")
                        .format("yyyy-MM-dd")
                        .fixedInterval(DateHistogramInterval.hours(div))
                        .subAggregation(AggregationBuilders
                                .sum("SUM")
                                .field("DataUsed")
                        );

        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            System.out.println(searchRequest);
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new RuntimeException();
        }
        Map<Map<String, Double>, Double> mapValue = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            Map<String, Double> mapOfKeyAsStringDocCount = new HashMap<>();
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            double docCount = agg.getBuckets().get(i).getDocCount();
            mapOfKeyAsStringDocCount.put(keyAsString, docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                System.out.println(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                mapValue.put(mapOfKeyAsStringDocCount, (Double) obj.get("value"));

            }
        }
        return mapValue;
    }

    public Object getHisAggregationByMultipleFieldsAndPoints(BigInteger startDate, BigInteger endDate, String operation, Integer points, Map<String, String> multipleFields) {
        switch (operation.toUpperCase()) {
            case "SUM":
                return getSumFromHistogramRange(startDate, endDate, points, multipleFields);
            case "AVG":
                return getAvgFromHistogramRange(startDate, endDate, points, multipleFields);
            case "MAX":
                return getMaxFromHistogramRange(startDate, endDate, points, multipleFields);
            case "MIN":
                return getMinFromHistogramRange(startDate, endDate, points, multipleFields);
        }
        return null;
    }

    private Object getMinFromHistogramRange(BigInteger startDate, BigInteger endDate, Integer points, Map<String, String> multipleFields) {
        List<Object> res = new ArrayList<>();
        Set<Map<String, Object>> listTillNow = new HashSet<>();
        Map<String, Object> mapOfFieldsAndList = new HashMap<>();
        String field = null;
        for (Map.Entry<String, String> entry : multipleFields.entrySet()) {
            field = entry.getValue();
            res = getMinAggregationForField(startDate, endDate, points, field);
            mapOfFieldsAndList.put(field, res);
        }
        listTillNow.add(mapOfFieldsAndList);
        return listTillNow;
    }

    private List<Object> getMinAggregationForField(BigInteger startDate, BigInteger endDate, Integer points, String field) {

        BigInteger differenceBetweenMillis = endDate.subtract(startDate);
        BigInteger interval = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1)));
        BigInteger div = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1) * 1000));
        System.out.println("interval==>" + interval);

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("inMillis").gte(startDate).lte(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("inMillis")
                        .fixedInterval(DateHistogramInterval.seconds(div.intValue()))
                        .subAggregation(AggregationBuilders.min(field).field(field));
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Entered field is not compatible for the aggregation \n" +
                    "try by changing the field");
        }
        return getMinOfDataPoints(searchResponse, interval);
    }

    private List<Object> getMinOfDataPoints(SearchResponse searchResponse, BigInteger interval) {
        System.out.println(searchResponse);

        Map<String, Object> fieldAndJsonObject = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");

        String fieldName = null;
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            BigInteger fromDate = new BigInteger(keyAsString);
            TimeStamp timeStamp = new TimeStamp();
            timeStamp.setFromDate(fromDate);
            timeStamp.setToDate(fromDate.add(interval));
            MinResponse minResponse = new MinResponse();
            minResponse.setTimeStamp(timeStamp);
            double docCount = agg.getBuckets().get(i).getDocCount();
            minResponse.setDocCount(docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                fieldName = String.valueOf(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                minResponse.setMin(obj.get("value"));
            }
            list.add(minResponse);
        }
        return list;

    }

    private Object getMaxFromHistogramRange(BigInteger startDate, BigInteger endDate, Integer points, Map<String, String> multipleFields) {

        List<Object> res = new ArrayList<>();
        Set<Map<String, Object>> listTillNow = new HashSet<>();
        Map<String, Object> mapOfFieldsAndList = new HashMap<>();
        String field = null;
        for (Map.Entry<String, String> entry : multipleFields.entrySet()) {
            field = entry.getValue();
            res = getMaxAggregationForField(startDate, endDate, points, field);
            mapOfFieldsAndList.put(field, res);
        }
        listTillNow.add(mapOfFieldsAndList);
        return listTillNow;
    }

    private List<Object> getMaxAggregationForField(BigInteger startDate, BigInteger endDate, Integer points, String field) {
        BigInteger differenceBetweenMillis = endDate.subtract(startDate);
        BigInteger interval = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1)));
        BigInteger div = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1) * 1000));
        System.out.println("interval==>" + interval);

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("inMillis").gte(startDate).lte(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("inMillis")
                        .fixedInterval(DateHistogramInterval.seconds(div.intValue()))
                        .subAggregation(AggregationBuilders.max(field).field(field));
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Entered field is not compatible for the aggregation \n" +
                    "try by changing the field");
        }
        return getMaxOfDataPoints(searchResponse, interval);
    }

    private List<Object> getMaxOfDataPoints(SearchResponse searchResponse, BigInteger interval) {
        System.out.println(searchResponse);

        Map<String, Object> fieldAndJsonObject = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");

        String fieldName = null;
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            BigInteger fromDate = new BigInteger(keyAsString);
            TimeStamp timeStamp = new TimeStamp();
            timeStamp.setFromDate(fromDate);
            timeStamp.setToDate(fromDate.add(interval));
            MaxResponse maxResponse = new MaxResponse();
            maxResponse.setTimeStamp(timeStamp);
            double docCount = agg.getBuckets().get(i).getDocCount();
            maxResponse.setDocCount(docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                fieldName = String.valueOf(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                maxResponse.setMax(obj.get("value"));
            }
            list.add(maxResponse);
        }
        return list;
    }

    private Object getAvgFromHistogramRange(BigInteger startDate, BigInteger endDate, Integer points, Map<String, String> multipleFields) {

        List<Object> res = new ArrayList<>();
        Set<Map<String, Object>> listTillNow = new HashSet<>();
        Map<String, Object> mapOfFieldsAndList = new HashMap<>();
        String field = null;
        for (Map.Entry<String, String> entry : multipleFields.entrySet()) {
            field = entry.getValue();
            res = getAvgAggregationForField(startDate, endDate, points, field);
            mapOfFieldsAndList.put(field, res);
        }
        listTillNow.add(mapOfFieldsAndList);
        return listTillNow;
    }

    private List<Object> getAvgAggregationForField(BigInteger startDate, BigInteger endDate, Integer points, String field) {

        BigInteger differenceBetweenMillis = endDate.subtract(startDate);
        BigInteger interval = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1)));
        BigInteger div = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1) * 1000));
        System.out.println("interval==>" + interval);

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("inMillis").gte(startDate).lte(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("inMillis")
                        .fixedInterval(DateHistogramInterval.seconds(div.intValue()))
                        .subAggregation(AggregationBuilders.avg(field).field(field));
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Entered field is not compatible for the aggregation \n" +
                    "try by changing the field");
        }
        return getAvgOfDataPoints(searchResponse, interval);
    }

    private List<Object> getAvgOfDataPoints(SearchResponse searchResponse, BigInteger interval) {
        System.out.println(searchResponse);

        Map<String, Object> fieldAndJsonObject = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");

        String fieldName = null;
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            BigInteger fromDate = new BigInteger(keyAsString);
            TimeStamp timeStamp = new TimeStamp();
            timeStamp.setFromDate(fromDate);
            timeStamp.setToDate(fromDate.add(interval));
            AvgResponse avgResponse = new AvgResponse();
            avgResponse.setTimeStamp(timeStamp);
            double docCount = agg.getBuckets().get(i).getDocCount();
            avgResponse.setDocCount(docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                fieldName = String.valueOf(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                avgResponse.setAvg(obj.get("value"));
            }
            list.add(avgResponse);
        }
        return list;
    }

    private Object getSumFromHistogramRange(BigInteger startDate, BigInteger endDate, Integer points, Map<String, String> multipleFields) {
        List<Object> res = new ArrayList<>();
        Set<Map<String, Object>> listTillNow = new HashSet<>();
        Map<String, Object> mapOfFieldsAndList = new HashMap<>();
        String field = null;
        for (Map.Entry<String, String> entry : multipleFields.entrySet()) {
            field = entry.getValue();
            res = getSumAggregationForField(startDate, endDate, points, field);
            mapOfFieldsAndList.put(field, res);
        }
        listTillNow.add(mapOfFieldsAndList);
        return listTillNow;
    }

    private List<Object> getSumAggregationForField(BigInteger startDate, BigInteger endDate, Integer points, String field) {
        BigInteger differenceBetweenMillis = endDate.subtract(startDate);
        BigInteger interval = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1)));
        BigInteger div = differenceBetweenMillis.divide(BigInteger.valueOf((points - 1) * 1000));
        System.out.println("interval==>" + interval);

        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("inMillis").gte(startDate).lte(endDate);
        SearchRequest searchRequest = new SearchRequest(ES_config.indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(rangeQueryBuilder);
        AggregationBuilder aggregation =
                AggregationBuilders
                        .dateHistogram("aggs")
                        .field("inMillis")
                        .order(BucketOrder.key(true))
                        .fixedInterval(DateHistogramInterval.seconds(div.intValue()))
                        .subAggregation(AggregationBuilders.sum(field).field(field));
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        } catch (Exception e) {
            throw new InvalidFieldEnteredException("Entered field is not compatible for the aggregation \n" +
                    "try by changing the field");
        }
        return getDataPoints(searchResponse, interval);
    }

    private List<Object> getDataPoints(SearchResponse searchResponse, BigInteger div) {
        System.out.println(searchResponse);

        Map<String, Object> fieldAndJsonObject = new HashMap<>();
        Histogram agg = searchResponse.getAggregations().get("aggs");

        String fieldName = null;
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < agg.getBuckets().size(); i++) {
            String keyAsString = agg.getBuckets().get(i).getKeyAsString();
            BigInteger fromDate = new BigInteger(keyAsString);
            TimeStamp timeStamp = new TimeStamp();
            timeStamp.setFromDate(fromDate);
            timeStamp.setToDate(fromDate.add(div));
            sumResponse response = new sumResponse();
            response.setTimeStamp(timeStamp);
            double docCount = agg.getBuckets().get(i).getDocCount();
            response.setDocCount(docCount);
            Map<String, Aggregation> valueMap = agg.getBuckets().get(i).getAggregations().asMap();
            for (Map.Entry<String, Aggregation> curr : valueMap.entrySet()) {
                fieldName = String.valueOf(curr.getKey());
                JSONObject obj = new JSONObject(curr.getValue());
                response.setSum(obj.get("value"));
            }
            list.add(response);
        }
        return list;
    }

}

