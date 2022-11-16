package com.elastiSearchClient.services;


import com.elastiSearchClient.repository.ElasticSearchRepository;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.core.MainResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ElasticSearchServices {
    @Autowired
    private ElasticSearchRepository repository;
    public SearchResponse getAllDocument() {
        return repository.getAllDocOnIndex();
    }

    public long getDocumentCount(){
        return repository.getDocCountOnIndex();
    }

    public GetResponse getById(String doc_id) {
        return repository.getDocById(doc_id);
    }
    public SearchResponse getDocumentByMatchQuery(String field, String textTobeSearch) {
        return repository.executeMatchQuery(field,textTobeSearch);
    }

    public Double getSumOfDataUsedBetweenTwoDates(String startDate, String endDate) {
        return repository.getSumOfDataUserd(startDate,endDate);
    }

    public Double getAvgOfDataUsedBetweenTwoDates(String startDate, String endDate) {
        return repository.getAvgOfDataUsed(startDate,endDate);
    }

    public SearchResponse getMaxOfDataUsedBetweenTwoDates(String startDate, String endDate) {
        return repository.getDeviceWhoUsedMaxData(startDate,endDate);
    }

    public SearchResponse getMinOfDataUsedBetweenTwoDates(String startDate, String endDate) {
        return repository.getDeviceWhoUsedMinData(startDate,endDate);
    }

    public SearchResponse getSumOfFieldsBetweenTwoDates(String startDate, String endDate, String field) {
        return repository.getSumForSpecificField(startDate,endDate,field);
    }

    public Double getAvgOfFieldsBetweenTwoDates(String startDate, String endDate, String field) {
        return repository.getAvgForSpecificField(startDate,endDate,field);
    }


    public MainResponse getInfoLog() {
        return repository.getInfoLog();
    }

    public List<String> getDocumentByRadioType() {
        return repository.getRadioType();
    }

    public Map<String, Long> getAggByOperators() {
        return repository.getAggregationByOperators();
    }

    public Map<String, Long> termAggByField(String field) {
        return repository.termAggByField(field);
    }

    public Map<Map<String, Double>, Double> getHistogramAggregationByDate() {
        return  repository.getHistogramByDateMaxDataUsed();
    }
    public boolean deleteIndex() {
        return repository.DeleteByIndex();
    }
}
