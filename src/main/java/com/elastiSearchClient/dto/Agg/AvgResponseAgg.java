package com.elastiSearchClient.dto.Agg;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

@Data
public class AvgResponseAgg {
    @Autowired
    private MonthStamp monthStamp;
    private Long docCount;
    private Object avg;
    @Override
    public String toString() {
        return "{" + ""+ monthStamp +
                ", docCount=" + docCount +
                ", avg=" + avg +
                '}';
    }
}
