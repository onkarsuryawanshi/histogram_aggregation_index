package com.elastiSearchClient.dto;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

@Data
public class sumResponse {
    @Autowired
    private TimeStamp timeStamp;

    private Double docCount;
    private Object sum;
}
