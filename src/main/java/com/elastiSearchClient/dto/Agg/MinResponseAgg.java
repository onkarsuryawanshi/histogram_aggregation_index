package com.elastiSearchClient.dto.Agg;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;

@Data
public class MinResponseAgg {
    @Autowired
    private MonthStamp monthStamp;
    private Long docCount;
    private Object min;

    @Override
    public String toString() {
        return "{" + ""+ monthStamp +
                ", docCount=" + docCount +
                ", min=" + min +
                '}';
    }
}
