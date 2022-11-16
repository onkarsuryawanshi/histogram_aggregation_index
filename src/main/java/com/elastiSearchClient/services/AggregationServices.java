package com.elastiSearchClient.services;


import com.elastiSearchClient.repository.AggregationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AggregationServices {

    @Autowired
    private AggregationRepository aggregationRepo;
    public Object aggregationByHistogramByUserField(String startDate, String endDate, String field, String operation) {

        return aggregationRepo.getAggByUserField(startDate,endDate,field,operation);
    }
}
