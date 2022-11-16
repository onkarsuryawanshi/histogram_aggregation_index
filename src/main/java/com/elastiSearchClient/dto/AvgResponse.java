package com.elastiSearchClient.dto;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

@Data
public class AvgResponse {
    @Autowired
    private TimeStamp timeStamp;
    private Double docCount;
    private Object avg;
}
