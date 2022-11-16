package com.elastiSearchClient.services;

import com.elastiSearchClient.repository.HistogramRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.Map;

@Service
public class HistogramServices {

    @Autowired
    private HistogramRepository histogramRepo;

    public Object createHistogram(String startDate, String endDate, String operation,String field) {
        return histogramRepo.getAggregationOperationsByUserField(startDate,endDate,operation,field);
    }

    public Map<String, Map<String, Double>> getHistogramAggregation(String startDate, String endDate, Integer points) {
        return histogramRepo.getAggregationByPoint(startDate,endDate,points);
    }

    public Map<Map<String, Double>, Double> createHistogram2(String startDate, String endDate, Integer points, String operation) {
        return histogramRepo.getAggByPointsAndOperations(startDate,endDate,points,operation);
    }

    public Object createHistogramForMultipleFieldsAndPointsEntered(BigInteger startDate, BigInteger endDate, String operation,Integer points, Map<String, String> multipleFields) {
            return histogramRepo.getHisAggregationByMultipleFieldsAndPoints(startDate,endDate,operation,points,multipleFields);
    }
}
