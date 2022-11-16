package com.elastiSearchClient.dto.Agg;

import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
@Data
public class SumResponseAgg {
    @Autowired
    private MonthStamp monthStamp;
    private Long docCount;
    private Object sum;

    @Override
    public String toString() {
        return "{" + ""+ monthStamp +
                ", docCount=" + docCount +
                ", sum=" + sum +
                '}';
    }
}
