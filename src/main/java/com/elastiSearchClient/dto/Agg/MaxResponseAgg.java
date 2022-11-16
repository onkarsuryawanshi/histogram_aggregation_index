package com.elastiSearchClient.dto.Agg;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;


@Data
public class MaxResponseAgg {

    @Autowired
    private MonthStamp monthStamp;
    private Long docCount;
    private Object max;

    @Override
    public String toString() {
        return "{" + ""+ monthStamp +
                ", docCount=" + docCount +
                ", max=" + max +
                '}';
    }
}
